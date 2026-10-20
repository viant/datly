package generate

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/packageasset"
	"github.com/viant/datly/internal/readergraph"
	"github.com/viant/datly/internal/typeidentity"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	sqltext "github.com/viant/sqlparser/source"
	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

func (r *planResolver) prepareReaderMetadata() error {
	p := r.plan
	reason, err := r.readerMetadataCapability()
	if err != nil {
		return err
	}
	if reason != "" {
		if r.readerMetadataRequired {
			return fmt.Errorf("reader metadata refresh required: %s", reason)
		}
		return nil
	}
	// Metadata filtering, resource corrections and ownership receipts are staged;
	// no failed metadata selection may mutate the resolved ordinary resource plan.
	originalResources, originalPrevious, originalURI := p.Resources, p.readerMetadataPrevious, p.ReaderMetadata
	staged := *p.Resources
	staged.Files = append([]EmittedFile(nil), p.Resources.Files...)
	p.Resources = &staged
	p.readerMetadataPrevious = map[string][32]byte{}
	for name, sum := range originalPrevious {
		p.readerMetadataPrevious[name] = sum
	}
	committed := false
	defer func() {
		if !committed {
			p.Resources, p.readerMetadataPrevious, p.ReaderMetadata = originalResources, originalPrevious, originalURI
		}
	}()
	if err := r.filterReaderMetadataResources(p.Resources); err != nil {
		return err
	}
	graph := r.input.Component.Clone()
	graph.ReaderGraph = nil
	graph.SelectorCompanion = nil
	graph.Key = spec.Key{Kind: spec.KindComponent, Scope: p.Package, Name: p.ComponentName}
	graph.Name = p.ComponentName
	graph.Settings.Generation = nil
	graph.Settings.InputType = p.Input.DescriptorKey
	graph.Settings.OutputType = p.Output.DescriptorKey
	graph.Settings.Report = p.Report.Clone()
	if len(graph.Routes) != 1 {
		return fmt.Errorf("reader metadata requires exact native route")
	}
	graph.Routes[0].Method = p.Routes[0].Method
	graph.Routes[0].Path = p.Routes[0].Path
	input, err := r.types.Descriptor(p.Input.DescriptorKey)
	if err != nil {
		return err
	}
	output, err := r.types.Descriptor(p.Output.DescriptorKey)
	if err != nil {
		return err
	}
	if err = bootstrap.CompileReaderGraphColumns(graph, input, output, r.types.Descriptor); err != nil {
		return err
	}
	if err = readergraph.Contracts(graph, input, output, r.types.Descriptor); err != nil {
		return err
	}
	files := map[string]string{}
	for _, file := range p.Resources.Files {
		files[file.Path] = file.Content
	}
	plans := map[string]*ViewPlan{}
	for i := range p.Views {
		plans[p.Views[i].Identity] = &p.Views[i]
	}
	rows := map[string]string{}
	sources := map[*spec.View]string{}
	// Resolve root/independent URI bindings through the exact retained contract fields.
	for _, param := range graph.Parameters {
		if param == nil {
			continue
		}
		var view *spec.View
		var contract ContractPlan
		if param.Source.Kind == "output" && param.Source.Name == "view" {
			view = graph.RootView
			contract = p.Output
		} else if param.Source.Kind == "view" {
			contract = p.Input
			for _, v := range graph.Views {
				if v.Name == param.Source.Name || v.Key.Name == param.Source.Name {
					if view != nil {
						return fmt.Errorf("ambiguous independent reader resource")
					}
					view = v
				}
			}
		}
		if view == nil {
			continue
		}
		field, ok := contract.Field(param.Name)
		if !ok {
			return fmt.Errorf("reader metadata missing exact resource field %s", param.Name)
		}
		metadata, err := tag.ParseField(reflect.StructField{Name: field.Name, Tag: reflect.StructTag(field.Tag)})
		if err != nil {
			return err
		}
		if metadata.SQL == nil || metadata.SQL.URI == "" {
			return fmt.Errorf("reader metadata field %s has no established SQL URI", param.Name)
		}
		sources[view] = qualifyReaderURI(p.Resources.Namespace, metadata.SQL.URI)
	}
	active := map[*spec.View]bool{}
	var walk func(*spec.View, string, *x.Type) error
	walk = func(v *spec.View, uri string, inheritedRow *x.Type) error {
		if v == nil {
			return nil
		}
		if active[v] {
			return fmt.Errorf("reader metadata cycle")
		}
		active[v] = true
		defer delete(active, v)
		identity, err := v.Identity()
		if err != nil {
			return err
		}
		planned := plans[identity]
		descriptor := inheritedRow
		if planned != nil {
			if planned.Ownership != ViewLinked {
				return fmt.Errorf("reader metadata view %s lacks linked row authority", identity)
			}
			descriptor, err = r.types.Descriptor(planned.DescriptorKey)
			if err != nil {
				return err
			}
			if inheritedRow != nil && descriptor.Key() != inheritedRow.Key() {
				return fmt.Errorf("reader metadata conflicting child row authority")
			}
		}
		if descriptor != nil && v.TypeName != "" {
			imports := map[string]string{}
			if graph.TypeContext != nil {
				for _, imported := range graph.TypeContext.Imports {
					imports[imported.Alias] = imported.Package
				}
			}
			declared, err := (xshape.Resolver{Package: p.Package, Imports: imports}).Canonical(v.TypeName)
			if err != nil {
				return err
			}
			if declared != descriptor.Key() {
				return fmt.Errorf("reader metadata declared row type differs from holder field")
			}
		}

		if descriptor == nil || descriptor.PkgPath != p.Package {
			return fmt.Errorf("reader metadata row must belong to exact holder package")
		}
		v.TypeName = descriptor.Key()
		rows[identity] = descriptor.Key()
		if !v.InMemory {
			if uri == "" {
				return fmt.Errorf("reader metadata view %s has no established SQL asset", identity)
			}
			namespace, relative, ok := strings.Cut(uri, ":")
			if !ok || namespace != p.Resources.Namespace || !fs.ValidPath(relative) {
				return fmt.Errorf("reader metadata view %s uses foreign resource", identity)
			}
			body, exists := files[relative]
			if !exists {
				return fmt.Errorf("reader metadata SQL asset %s is not materialized", uri)
			}
			canonical := v.RuntimeSource().Clone()
			if canonical == nil {
				return fmt.Errorf("reader metadata view %s has no source", identity)
			}
			if err = dsql.ResolveSource(v.Name, canonical, r.input.Resources); err != nil {
				return err
			}
			// Linked application assets remain byte-owned even when the ordinary
			// resource plan proposed a transparent wrapper. Admit only an existing
			// confined asset, checked against the selected source filesystem.
			if r.input.ProjectRoot != "" {
				directory, err := r.readerMetadataDirectory()
				if err != nil {
					return err
				}
				root, err := os.OpenRoot(directory)
				if err != nil {
					return err
				}
				original, readErr := fs.ReadFile(root.FS(), relative)
				root.Close()
				if readErr != nil {
					return fmt.Errorf("reader metadata original SQL asset %s is unavailable: %w", relative, readErr)
				}
				if !readerSQLMaterializes(canonical.SQL, string(original)) {
					return fmt.Errorf("reader metadata original SQL asset %s changes canonical semantics", relative)
				}
				// The current typed SQL resource authority must observe the same
				// application bytes; pathname presence alone grants no authority.
				observed, observeErr := fs.ReadFile(r.input.Resources, uri)
				if observeErr != nil {
					observed, observeErr = fs.ReadFile(r.input.Resources, relative)
				}
				if observeErr != nil || string(observed) != string(original) {
					return fmt.Errorf("reader metadata SQL asset %s differs from selected source authority", relative)
				}
				body = string(original)
				files[relative] = body
				for i := range p.Resources.Files {
					if p.Resources.Files[i].Path == relative {
						p.Resources.Files[i].Content = body
					}
				}
			}
			if !readerSQLMaterializes(canonical.SQL, body) {
				return fmt.Errorf("reader metadata SQL asset %s does not faithfully materialize canonical source", uri)
			}
			canonical.SQL = ""
			canonical.URI = uri
			canonical.Embeds = nil
			v.Source = canonical
		} else {
			v.Source = v.RuntimeSource().Clone()
		}
		shape := xshape.New(descriptor, r.types.Descriptor)
		for _, rel := range v.Relations {
			if rel == nil || rel.View == nil {
				return fmt.Errorf("incomplete reader metadata relation")
			}
			if rel.Kind == spec.RelationKindDerived {
				return fmt.Errorf("reader metadata derived output URI binding is not yet supported")
			}
			fields, err := shape.Fields()
			if err != nil {
				return err
			}
			var selected *xshape.Field
			for i := range fields {
				if fields[i].Name == rel.Holder {
					selected = &fields[i]
				}
			}
			if selected == nil {
				return fmt.Errorf("reader metadata relation holder %s is missing", rel.Holder)
			}
			metadata, err := tag.ParseField(selected.StructField())
			if err != nil {
				return err
			}
			childURI := ""
			if metadata.SQL != nil {
				childURI = metadata.SQL.URI
			}
			if childURI == "" && !rel.View.InMemory {
				return fmt.Errorf("reader metadata relation %s has no established SQL URI", rel.Holder)
			}
			resolved, err := shape.ResolveField(rel.Holder)
			if err != nil {
				return err
			}
			if err = walk(rel.View, qualifyReaderURI(p.Resources.Namespace, childURI), resolved.Descriptor); err != nil {
				return err
			}
			rel.Tag = withoutStructTags(rel.Tag, tag.SQLName, tag.ViewName, tag.RelationName, "self")
		}
		columns := v.Columns[:0]
		for _, column := range v.Columns {
			// Metadata carries the native scan projection; SQL-transient fields
			// remain owned by hooks and presentation on the linked public row.
			if column != nil && sqlxio.ParseTag(reflect.StructTag(column.Tag)).Transient {
				continue
			}
			if column != nil {
				// SQL inference describes the source; an established linked scalar
				// pointer is also a valid Go destination for the same base type.
				// Preserve physical nullability and explicit CAST authority.
				typ := column.EffectiveType()
				if !column.ExplicitType && !typ.Pointer && !typ.SlicePointer && typ.Cardinality != spec.CardinalityMany {
					expression := typ.Name
					if typ.Package != "" {
						expression = typ.Package + "." + expression
					}
					scope := xshape.Resolver{Package: descriptor.PkgPath}
					if reference, referenceErr := scope.Reference(expression); referenceErr == nil && len(reference.Wrappers) == 0 {
						canonical, canonicalErr := scope.Canonical(expression)
						if canonicalErr != nil {
							return canonicalErr
						}
						field, fieldErr := readergraph.ResolveRowColumn(shape, column)
						if fieldErr != nil {
							return fieldErr
						}
						if field.Identity == "*"+canonical && readerMetadataScalar(field.Descriptor, r.types.Descriptor, map[string]bool{}) {
							column.Type.Pointer = true
						}
					}
				}
				column.Tag = withoutStructTags(column.Tag, tag.SQLName, tag.ViewName, tag.RelationName, "self", "groupable", "codec")
			}
			columns = append(columns, column)
		}
		v.Columns = columns
		return nil
	}
	if err = walk(graph.RootView, sources[graph.RootView], nil); err != nil {
		return err
	}
	for _, v := range graph.Views {
		if err = walk(v, sources[v], nil); err != nil {
			return err
		}
	}
	sideTags := make(map[string]string, len(p.QuerySelectors))
	for _, field := range p.QuerySelectors {
		metadata, err := tag.ParseField(reflect.StructField{Name: field.Name, Tag: reflect.StructTag(field.Tag)})
		if err != nil {
			return err
		}
		identity := (&spec.Parameter{Name: metadata.Binding.Name, Source: spec.BindSource{Kind: metadata.Binding.Location.Kind, Name: metadata.Binding.Location.In}}).Identity()
		sideTags[identity] = field.Tag
	}
	for _, param := range graph.Parameters {
		if param != nil {
			// Persist the exact Go destination while source alias declarations are
			// available; reflected runtime types no longer carry those aliases.
			if err := typeidentity.NormalizeDestination(param, p.Package, graph.TypeContext, r.types.Descriptor); err != nil {
				return err
			}
			if value, planned := sideTags[param.Identity()]; planned {
				param.Tag = value
			}
			param.Tag = withoutStructTags(param.Tag, tag.SQLName, tag.ViewName, tag.RelationName, "self")
		}
	}
	signature, err := readergraph.Signature(input, output, r.types.Descriptor)
	if err != nil {
		return err
	}
	document := readergraph.Document{ABI: signature, Version: 1, Package: p.Package, Holder: p.HolderName(), Field: "Contract", Input: input.Key(), Output: output.Key(), Rows: rows, Component: graph}
	if err = readergraph.CheckRows(&document, r.types.Descriptor); err != nil {
		return err
	}
	body, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return err
	}
	if _, err = readergraph.StrictDecode(body); err != nil {
		return err
	}
	filename := path.Join("metadata", lowerSnake(p.ComponentName)+"_reader.json")
	if _, exists := files[filename]; exists {
		return fmt.Errorf("reader metadata asset %s already occupies resource plan", filename)
	}
	if r.input.ProjectRoot != "" {
		directory, err := r.readerMetadataDirectory()
		if err != nil {
			return err
		}
		root, err := os.OpenRoot(directory)
		if err == nil {
			_, err = fs.Stat(root.FS(), filename)
			root.Close()
			if err == nil {
				if _, owned := p.readerMetadataPrevious[filename]; !owned {
					return fmt.Errorf("reader metadata destination %s has no current generated ownership", filename)
				}
			} else if !os.IsNotExist(err) {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	p.ReaderMetadata = p.Resources.Namespace + ":" + filename
	p.Resources.Files = append(p.Resources.Files, EmittedFile{Path: filename, Content: string(body) + "\n"})
	p.Resources.sourceText = ""
	committed = true
	sort.Slice(p.Resources.Files, func(i, j int) bool { return p.Resources.Files[i].Path < p.Resources.Files[j].Path })
	return nil
}
func qualifyReaderURI(namespace, uri string) string {
	if uri == "" || strings.Contains(uri, ":") {
		return uri
	}
	return namespace + ":" + uri
}

// Positive admission accepts exact bytes or a single modifier-free wildcard
// wrapper. In particular Qualify is sqlparser's WHERE field, not QUALIFY.
func readerSQLMaterializes(canonical, asset string) bool {
	if strings.TrimSpace(canonical) == strings.TrimSpace(asset) {
		return true
	}
	q, err := sqlparser.ParseQuery(canonical)
	if err != nil || q == nil || len(q.List) != 1 || len(q.Joins) > 0 || len(q.WithSelects) > 0 || q.WithRecursive || q.Union != nil || len(q.GroupBy) > 0 || q.Having != nil || len(q.OrderBy) > 0 || q.Limit != nil || q.Offset != nil || q.Qualify != nil || q.QualifyClause != nil || q.Kind != "" || q.Window != nil {
		return false
	}
	star, ok := q.List[0].Expr.(*expr.Star)
	if !ok || len(star.Except) > 0 || q.List[0].Alias != "" {
		return false
	}
	if star.X != nil {
		identifier, ok := star.X.(*expr.Ident)
		if !ok || (identifier.Name != "*" && identifier.Name != q.From.Alias) {
			return false
		}
	}
	raw, ok := q.From.X.(*expr.Raw)
	if !ok {
		return false
	}
	inner := strings.TrimSpace(raw.Raw)
	if group, end, ok := sqltext.ReadGroupString(inner, 0, '(', ')'); ok && end == len(inner) {
		inner = strings.TrimSpace(group[1 : len(group)-1])
	}
	assetSQL := strings.TrimSpace(sqltext.MaskLineComments(asset))
	if group, end, ok := sqltext.ReadGroupString(assetSQL, 0, '(', ')'); ok && end == len(assetSQL) {
		assetSQL = strings.TrimSpace(group[1 : len(group)-1])
	}
	return strings.TrimSpace(inner) == assetSQL
}

func (r *planResolver) readerMetadataDirectory() (string, error) {
	authority, err := typecatalog.NewDestinationAuthority(r.input.ProjectRoot)
	if err != nil {
		return "", err
	}
	destination, err := authority.Resolve(r.plan.Package, "")
	if err != nil {
		return "", err
	}
	return filepath.Join(r.input.ProjectRoot, destination.Directory), nil
}

// Only a current generated holder and generated embed declaration can identify
// replaceable metadata. The JSON's names or a published digest grant no ownership.
func readerMetadataOwned(source fs.FS, p *Plan, resources *ResourcePlan, uri string) ([]byte, error) {
	namespace, filename, ok := strings.Cut(uri, ":")
	if !ok || namespace != resources.Namespace || !fs.ValidPath(filename) {
		return nil, fmt.Errorf("reader metadata ownership namespace mismatch")
	}
	found := 0
	names, err := fs.Glob(source, "*.go")
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		body, err := fs.ReadFile(source, name)
		if err != nil {
			return nil, err
		}
		if generatedOwner(body) != p.ComponentName {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), name, body, 0)
		if err != nil {
			return nil, err
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, entry := range gen.Specs {
				typ, ok := entry.(*ast.TypeSpec)
				if !ok || typ.Name.Name != p.HolderName() {
					continue
				}
				structure, ok := typ.Type.(*ast.StructType)
				if !ok {
					continue
				}
				for _, field := range structure.Fields.List {
					if len(field.Names) != 1 || field.Names[0].Name != "Contract" || field.Tag == nil {
						continue
					}
					raw, err := strconv.Unquote(field.Tag.Value)
					if err != nil {
						return nil, err
					}
					metadata, present, err := tag.ParseComponent(reflect.StructTag(raw))
					if err != nil {
						return nil, err
					}
					if present && metadata.Name == p.ComponentName && metadata.ReaderMetadata == uri {
						found++
					}
				}
			}
		}
	}
	if found != 1 {
		return nil, fmt.Errorf("reader metadata replacement lacks one current generated holder")
	}
	declarations, err := packageasset.DiscoverSourceResourcesFS(source)
	if err != nil {
		return nil, err
	}
	embedded := false
	for _, d := range declarations {
		if d.Namespace != namespace {
			continue
		}
		body, err := fs.ReadFile(source, d.SourceFile)
		if err != nil {
			return nil, err
		}
		if generatedOwner(body) != p.ComponentName {
			return nil, fmt.Errorf("reader metadata resource declaration is application-owned")
		}
		for _, f := range d.Files {
			if f == filename {
				embedded = true
			}
		}
	}
	if !embedded {
		return nil, fmt.Errorf("reader metadata replacement lacks current generated embed ownership")
	}
	body, err := fs.ReadFile(source, filename)
	if err != nil {
		return nil, err
	}
	document, err := readergraph.StrictDecode(body)
	if err != nil {
		return nil, err
	}
	if document.Package != p.Package || document.Holder != p.HolderName() || document.Field != "Contract" || document.Component.Name != p.ComponentName || document.Input != p.Input.DescriptorKey || document.Output != p.Output.DescriptorKey {
		return nil, fmt.Errorf("reader metadata replacement has stale nominal ownership")
	}
	return body, nil
}
func (r *planResolver) filterReaderMetadataResources(resources *ResourcePlan) error {
	if r.input.ProjectRoot == "" || resources.sourceText == "" {
		return nil
	}
	directory, err := r.readerMetadataDirectory()
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(directory)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer root.Close()
	var retained []EmittedFile
	for _, file := range resources.Files {
		if !strings.HasPrefix(file.Path, "metadata/") || !strings.HasSuffix(file.Path, "_reader.json") {
			retained = append(retained, file)
			continue
		}
		document, decodeErr := readergraph.StrictDecode([]byte(file.Content))
		if decodeErr != nil || document.Package != r.plan.Package || document.Component.Name != r.plan.ComponentName {
			retained = append(retained, file)
			continue
		}
		body, err := readerMetadataOwned(root.FS(), r.plan, resources, resources.Namespace+":"+file.Path)
		if err != nil {
			return err
		}
		if r.plan.readerMetadataPrevious == nil {
			r.plan.readerMetadataPrevious = map[string][32]byte{}
		}
		r.plan.readerMetadataPrevious[file.Path] = sha256.Sum256(body)
	}
	if len(retained) != len(resources.Files) {
		resources.Files = retained
		if resources.authoredSource() {
			return fmt.Errorf("reader metadata cannot rewrite authored embed coverage")
		}
		resources.sourceText = ""
	}
	return nil
}

// validateReaderMetadataSource runs before static, shape discovery and resource
// gates. The URI is only a locator into an already captured immutable closure.
func (r *planResolver) validateReaderMetadataSource() error {
	baseline := r.input.ReaderMetadataSource
	marked := baseline != nil && baseline.ReaderGraph != nil
	if !marked {
		if r.input.ReaderMetadataURI != "" || (r.input.Component != nil && r.input.Component.ReaderGraph != nil) {
			return fmt.Errorf("reader metadata prior admitted source binding is required")
		}
		return nil
	}
	uri := r.input.ReaderMetadataURI
	if uri == "" {
		return fmt.Errorf("reader metadata exact admitted source URI is required")
	}
	namespace, filename, qualified := strings.Cut(uri, ":")
	if !qualified || namespace == "" || !fs.ValidPath(filename) || !strings.HasPrefix(filename, "metadata/") {
		return fmt.Errorf("reader metadata invalid admitted source URI")
	}
	if _, captured := baseline.ReaderGraph.Assets()[uri]; !captured {
		return fmt.Errorf("reader metadata source URI was not captured by prior admission")
	}
	if baseline.Settings == nil || r.types == nil {
		return fmt.Errorf("reader metadata prior contract authority required")
	}
	input, err := r.types.Descriptor(baseline.Settings.InputType)
	if err != nil {
		return err
	}
	output, err := r.types.Descriptor(baseline.Settings.OutputType)
	if err != nil {
		return err
	}
	if err = readergraph.ValidateWithLookup(baseline, input, output, r.input.Resources, r.types.Descriptor); err != nil {
		return err
	}
	body, err := fs.ReadFile(r.input.Resources, uri)
	if err != nil {
		return err
	}
	document, err := readergraph.StrictDecode(body)
	if err != nil {
		return err
	}
	baselineBytes, err := readergraph.Bytes(baseline)
	if err != nil {
		return err
	}
	documentBytes, err := readergraph.Bytes(document.Component)
	if err != nil {
		return err
	}
	if !bytes.Equal(baselineBytes, documentBytes) || document.Package != baseline.Key.Scope || document.Input != input.Key() || document.Output != output.Key() || document.Component.Key != baseline.Key || document.Component.Name != baseline.Name || document.Field != "Contract" || document.Holder == "" {
		return fmt.Errorf("reader metadata admitted document identity differs from source baseline")
	}
	current := r.input.Component
	if current == nil || current.Name != baseline.Name || current.Key != baseline.Key || r.input.TargetPackage != document.Package || len(current.Routes) != 1 || current.Routes[0] == nil || len(baseline.Routes) != 1 || current.Routes[0].Method != baseline.Routes[0].Method || current.Routes[0].Path != baseline.Routes[0].Path {
		return fmt.Errorf("reader metadata refresh target differs from admitted source")
	}
	if current.ReaderGraph != nil {
		if err := readergraph.ValidateWithLookup(current, input, output, r.input.Resources, r.types.Descriptor); err != nil {
			return err
		}
	}
	r.readerMetadataRequired = true
	r.readerMetadataDocument = document
	// Runtime type discovery is not shapes-only publication, but cannot change
	// the admitted execution owner or bypass a static/handler transition.
	if (r.requireConcreteHelpers && !r.input.SQLResources) || current.Static != nil || current.RootView == nil || r.input.ExternalHandler != nil || r.input.GoHandler != nil || r.input.ContractHandler != nil || r.input.MutationHandler != nil || r.input.VeltyHandler != nil {
		return fmt.Errorf("reader metadata refresh cannot leave ordinary SQL reader publication")
	}
	return nil
}

func (r *planResolver) checkReaderMetadataTarget() error {
	if !r.readerMetadataRequired {
		return nil
	}
	d, p := r.readerMetadataDocument, r.plan
	if p.Package != d.Package || p.ComponentName != d.Component.Name || p.HolderName() != d.Holder || p.Input.DescriptorKey != d.Input || p.Output.DescriptorKey != d.Output || p.Input.Ownership != ContractLinked || p.Output.Ownership != ContractLinked || (r.requireConcreteHelpers && p.ShapesOnly) {
		return fmt.Errorf("reader metadata exact holder and contract refresh target changed")
	}
	return nil
}

// Only capability exclusions return a reason. Bad declared URIs, missing assets,
// conflicting descriptors or source semantics remain validation errors after
// selection. No selector companion is required for this capability.
func (r *planResolver) readerMetadataCapability() (string, error) {
	p, c := r.plan, r.input.Component
	if !r.input.SQLResources || c.RootView == nil || p.ShapesOnly || p.Handler != "" || p.Input.Ownership != ContractLinked || p.Output.Ownership != ContractLinked || p.Input.Package != p.Package || p.Output.Package != p.Package {
		return "outside linked ordinary reader capability", nil
	}
	if c.Static != nil || len(p.Routes) != 1 || p.Routes[0].Method != "GET" || len(c.Views) != 0 {
		return "unsupported route or independent input view capability", nil
	}
	settings := c.Settings
	if settings == nil || settings.Mutation != "" || settings.SequenceStrategy != "" || settings.ComponentCallPolicy != "" || settings.IndependentChildTransactions || settings.WarmupTarget != nil || len(settings.ProtectedFlushTables) > 0 {
		return "unsupported execution ownership", nil
	}
	if p.Resources == nil || p.Resources.authoredSource() {
		return "no generator-owned resource declaration capability", nil
	}
	var rootHolder string
	for _, parameter := range spec.EffectiveParameters(c.Parameters) {
		if parameter != nil && parameter.Source.Kind == "output" && parameter.Source.Name == "view" {
			if rootHolder != "" {
				return "", fmt.Errorf("reader metadata ambiguous root holder")
			}
			rootHolder = parameter.Name
		}
	}
	if rootHolder == "" {
		return "no established root URI holder capability", nil
	}
	output, err := r.types.Descriptor(p.Output.DescriptorKey)
	if err != nil {
		return "", err
	}
	field, err := xshape.New(output, r.types.Descriptor).ResolveField(rootHolder)
	if err != nil {
		return "", err
	}
	metadataField, ok := p.Output.Field(rootHolder)
	if !ok {
		return "", fmt.Errorf("reader metadata exact root contract field is missing")
	}
	metadata, err := tag.ParseField(reflect.StructField{Name: metadataField.Name, Tag: reflect.StructTag(metadataField.Tag)})
	if err != nil {
		return "", err
	}
	uri := ""
	if metadata.SQL != nil {
		uri = metadata.SQL.URI
	}
	plans := map[string]*ViewPlan{}
	for i := range p.Views {
		plans[p.Views[i].Identity] = &p.Views[i]
	}
	seen := map[*spec.View]bool{}
	var visit func(*spec.View, *x.Type, string) (string, error)
	visit = func(view *spec.View, row *x.Type, uri string) (string, error) {
		if view == nil || row == nil {
			return "", fmt.Errorf("reader metadata exact row is missing")
		}
		if seen[view] {
			return "", fmt.Errorf("reader metadata ambiguous repeated graph occurrence")
		}
		seen[view] = true
		identity, err := view.Identity()
		if err != nil {
			return "", err
		}
		// Linked contracts can still contain SQL-owned generated rows after a
		// package reload. Optional metadata requires linked row authority too.
		if planned := plans[identity]; planned != nil && planned.Ownership != ViewLinked {
			return "generated row outside linked reader metadata capability", nil
		}
		if row.PkgPath != p.Package {
			return "foreign nominal row capability", nil
		}
		if !view.InMemory && uri == "" {
			return "no established URI materialization capability", nil
		}
		if uri != "" {
			qualified := qualifyReaderURI(p.Resources.Namespace, uri)
			ns, path, ok := strings.Cut(qualified, ":")
			if !ok || ns != p.Resources.Namespace || !fs.ValidPath(path) {
				return "", fmt.Errorf("reader metadata malformed or foreign declared resource URI")
			}
		}
		shape := xshape.New(row, r.types.Descriptor)
		for _, relation := range view.Relations {
			if relation == nil || relation.View == nil {
				return "", fmt.Errorf("reader metadata incomplete relation")
			}
			if relation.Kind == spec.RelationKindDerived {
				return "derived output relation capability", nil
			}
			resolved, err := shape.ResolveField(relation.Holder)
			if err != nil {
				return "", err
			}
			fields, err := shape.Fields()
			if err != nil {
				return "", err
			}
			childURI := ""
			for _, field := range fields {
				if field.Name == relation.Holder {
					metadata, err := tag.ParseField(field.StructField())
					if err != nil {
						return "", err
					}
					if metadata.SQL != nil {
						childURI = metadata.SQL.URI
					}
				}
			}
			reason, err := visit(relation.View, resolved.Descriptor, childURI)
			if err != nil || reason != "" {
				return reason, err
			}
		}
		return "", nil
	}
	return visit(c.RootView, field.Descriptor, uri)
}

// Named collections retain their identity, so syntax alone cannot distinguish
// them from SQL scalar destinations. Follow their existing type declaration.
func readerMetadataScalar(descriptor *x.Type, lookup xshape.Lookup, seen map[string]bool) bool {
	if descriptor == nil || seen[descriptor.Key()] {
		return false
	}
	seen[descriptor.Key()] = true
	if descriptor.Type != nil {
		switch descriptor.Type.Kind() {
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64, reflect.String, reflect.Struct:
			return true
		}
		return false
	}
	declaration := descriptor.SynteticType
	if declaration == nil || declaration.TypeSpec == nil {
		return false
	}
	switch expression := declaration.TypeSpec.Type.(type) {
	case *ast.StructType:
		return true
	case *ast.Ident, *ast.SelectorExpr:
		imports := map[string]string{}
		for alias, imported := range declaration.Imports {
			if imported != nil {
				imports[alias] = imported.Path
			}
		}
		canonical, err := canonicalType(expression, imports, descriptor.PkgPath)
		if err != nil {
			return false
		}
		target, err := lookup(canonical)
		return err == nil && readerMetadataScalar(target, lookup, seen)
	}
	return false
}
