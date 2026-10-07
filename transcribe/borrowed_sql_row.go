package transcribe

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/packageasset"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	handlercompiler "github.com/viant/datly/transcribe/handler/compiler"
	"github.com/viant/datly/typecatalog"
	sqlio "github.com/viant/sqlx/io"
	xmodule "github.com/viant/x/module"
)

func authoredBorrowedSQLRows(c *spec.Component) []spec.BorrowedSQLRow {
	if c == nil || c.Settings == nil || c.Settings.Generation == nil {
		return nil
	}
	return append([]spec.BorrowedSQLRow(nil), c.Settings.Generation.BorrowedSQLRows...)
}

// resolveBorrowedBodySlot resolves exact generated holder names. If the writer
// body is implicit, its existing canonical default is the component's name.
func resolveBorrowedBodySlot(c *spec.Component, path string) (*spec.View, []string, error) {
	if c == nil || c.RootView == nil {
		return nil, nil, fmt.Errorf("borrow_sql_row requires a canonical body graph")
	}
	parts := strings.Split(path, "/")
	rootName := typecatalog.FieldName(c.Name)
	bodyCount := 0
	for _, p := range spec.EffectiveParameters(c.Parameters) {
		if p != nil && strings.EqualFold(p.Source.Kind, "body") {
			bodyCount++
			rootName = typecatalog.FieldName(p.Name)
		}
	}
	if bodyCount > 1 || parts[0] != rootName {
		return nil, nil, fmt.Errorf("borrow_sql_row body path %q does not select the exact canonical body holder %q", path, rootName)
	}
	view := c.RootView
	id, err := view.Identity()
	if err != nil {
		return nil, nil, err
	}
	graph := []string{id}
	seen := map[*spec.View]bool{view: true}
	for _, holder := range parts[1:] {
		var found *spec.View
		for _, relation := range view.Relations {
			if relation == nil || relation.View == nil {
				return nil, nil, fmt.Errorf("borrow_sql_row body relation is incomplete")
			}
			name := relation.Holder
			if name == "" {
				name = relation.Name
			}
			if typecatalog.FieldName(name) == holder {
				if found != nil {
					return nil, nil, fmt.Errorf("borrow_sql_row holder %q is ambiguous", holder)
				}
				found = relation.View
			}
		}
		if found == nil || seen[found] {
			return nil, nil, fmt.Errorf("borrow_sql_row holder %q is missing or cyclic", holder)
		}
		view = found
		seen[view] = true
		id, err = view.Identity()
		if err != nil {
			return nil, nil, err
		}
		graph = append(graph, id)
	}
	if len(view.Relations) != 0 || view.SelfReference != nil {
		return nil, nil, fmt.Errorf("borrow_sql_row target is not a leaf")
	}
	return view, graph, nil
}

func borrowedLeafContract(ctx context.Context, result *Result, view *spec.View, declaration spec.BorrowedSQLRow) (gen.BorrowedLeafContract, error) {
	if result == nil || result.Source == nil || result.Source.ColumnRefiner == nil || view == nil || view.Source == nil {
		return gen.BorrowedLeafContract{}, fmt.Errorf("borrow_sql_row requires fresh physical source discovery")
	}
	// Discovery preserves authored URI/embed provenance. Resolve a detached leaf
	// from the same captured resources before proving its physical SQL ownership.
	physicalView := view.Clone()
	var resources fs.FS
	if result.Source.Resources != nil {
		resources = result.Source.Const.Resources(result.Source.Resources)
	}
	if err := dsql.ResolveSource(view.Name, physicalView.Source, resources); err != nil {
		return gen.BorrowedLeafContract{}, err
	}
	facts, err := result.Source.ColumnRefiner.PhysicalSourceConstraints(ctx, borrowedConnector(result.Component, view), physicalView)
	if err != nil {
		return gen.BorrowedLeafContract{}, err
	}
	origin := facts.Identity
	comparisonView := view
	if view.Auxiliary && view != result.Component.RootView && view.NestedNullPolicy == "skip-auxiliary" {
		// Validate the original collection role before detaching it. A shared row
		// comparison must not manufacture eligibility for an invalid policy.
		if err := (&gen.Input{Component: result.Component}).ValidateLifecycleTarget(true); err != nil {
			return gen.BorrowedLeafContract{}, err
		}
		// Independent row generation promotes this leaf to a root. Its nested
		// auxiliary traversal policy remains on the canonical/runtime occurrence,
		// while the physical owner's shared row and Has contract stays unchanged.
		comparisonView = view.Clone()
		comparisonView.NestedNullPolicy = ""
	}
	contract, err := gen.BorrowedLeafContractFor(result.Component, comparisonView, declaration.Package, declaration.Type, origin.Connector, origin.Catalog, origin.Schema, origin.Table)
	if err != nil {
		return contract, err
	}
	if err = applyBorrowedPhysicalFacts(&contract, facts); err != nil {
		return contract, err
	}
	path := declaration.BodyPath
	if result.Source.Name == declaration.OwnerName {
		path = declaration.OwnerBodyPath
	}
	contract.Wrapper, err = gen.BorrowedBodyWrapper(result.Component, path, declaration.Package)
	return contract, err
}

func borrowedConnector(c *spec.Component, v *spec.View) string {
	if v.Source != nil && v.Source.Bindings != nil && v.Source.Bindings.Connector != "" {
		return v.Source.Bindings.Connector
	}
	if c.Settings != nil {
		return c.Settings.DefaultConnector
	}
	return ""
}

// freshBorrowedSource has no body/package overlay or generated row catalog.
// The filesystem resources are captured as immutable bytes before discovery.
func freshBorrowedSource(ctx context.Context, source *Source, text string, files []gen.BorrowedAuthorityFile) (*Result, error) {
	copy := *source
	copy.Text = text
	copy.PackageComponent = nil
	copy.LinkedInputType = nil
	copy.LinkedOutputType = nil
	copy.Types = typecatalog.NewCatalog()
	var captured []packageasset.File
	for _, f := range files {
		rel, err := filepath.Rel(source.BaseDir(), f.Path)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, "../") {
			captured = append(captured, packageasset.File{Path: filepath.ToSlash(rel), Data: append([]byte(nil), f.Bytes...)})
		}
	}
	var err error
	snapshot, err := (packageasset.Snapshotter{Overlay: captured}).All(ctx)
	if err != nil {
		return nil, err
	}
	copy.Resources, err = resource.New().WithDefault(snapshot)
	if err != nil {
		return nil, err
	}
	return (&Compiler{comparisonOnly: true}).Compile(ctx, &copy)
}

func validateBorrowedSourceText(source *Source, files []gen.BorrowedAuthorityFile) error {
	for _, file := range files {
		if filepath.Clean(file.Path) == filepath.Clean(source.Path) {
			if string(file.Bytes) != source.Text {
				return fmt.Errorf("borrow_sql_row consumed DQL differs from captured source bytes")
			}
			return nil
		}
	}
	return fmt.Errorf("borrow_sql_row persistent authored source is missing")
}
func validateBorrowedConsumedFiles(consumed, current []gen.BorrowedAuthorityFile) error {
	if len(consumed) == 0 {
		return fmt.Errorf("borrow_sql_row consumed source/resource evidence is missing")
	}
	if len(consumed) != len(current) {
		return fmt.Errorf("borrow_sql_row source/resource membership changed before sealing")
	}
	lookup := map[string]gen.BorrowedAuthorityFile{}
	for _, file := range current {
		lookup[file.Path] = file
	}
	for _, file := range consumed {
		other, ok := lookup[file.Path]
		if !ok || file.Mode != other.Mode || !bytes.Equal(file.Bytes, other.Bytes) {
			return fmt.Errorf("borrow_sql_row consumed source/resource changed before sealing: %s", file.Path)
		}
	}
	return nil
}
func capturedBorrowedResources(source *Source, files []gen.BorrowedAuthorityFile) (*resource.Store, error) {
	var captured []packageasset.File
	for _, file := range files {
		rel, err := filepath.Rel(source.BaseDir(), file.Path)
		if err != nil {
			return nil, err
		}
		captured = append(captured, packageasset.File{Path: filepath.ToSlash(rel), Data: append([]byte(nil), file.Bytes...)})
	}
	snapshot, err := (packageasset.Snapshotter{Overlay: captured}).All(context.Background())
	if err != nil {
		return nil, err
	}
	resources := source.Resources
	if resources == nil {
		resources = resource.New()
	}
	return resources.WithDefault(snapshot)
}

func sealBorrowedSourcePackage(base string) ([]gen.BorrowedAuthorityFile, error) {
	var files []gen.BorrowedAuthorityFile
	err := filepath.WalkDir(base, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		extension := filepath.Ext(path)
		if extension != ".dql" && extension != ".sql" {
			return nil
		}
		f, err := gen.SealBorrowedAuthorityFile(path)
		if err != nil {
			return err
		}
		files = append(files, f)
		return nil
	})
	return files, err
}

func (c *Compiler) admitBorrowedSQLRows(ctx context.Context, result *Result) error {
	if c.comparisonOnly {
		return nil
	}
	declarations := result.AuthoredBorrowedSQLRows
	if len(declarations) == 0 {
		if len(authoredBorrowedSQLRows(result.Component)) != 0 {
			return fmt.Errorf("borrow_sql_row inherited settings cannot establish borrowing intent")
		}
		return nil
	}
	source := result.Source
	if source.ColumnRefiner == nil || source.BaseDir() == "" {
		return fmt.Errorf("borrow_sql_row requires current SQL schema discovery and an exact source resource directory")
	}
	if result.Component.TypeContext == nil {
		return fmt.Errorf("borrow_sql_row canonical package authority is missing")
	}
	files, err := sealBorrowedSourcePackage(source.BaseDir())
	if err != nil {
		return err
	}
	if err = validateBorrowedConsumedFiles(result.borrowedConsumedFiles, files); err != nil {
		return err
	}
	if err = validateBorrowedSourceText(source, files); err != nil {
		return err
	}
	freshBorrower, err := freshBorrowedSource(ctx, source, source.Text, files)
	if err != nil {
		return err
	}
	if len(authoredBorrowedSQLRows(freshBorrower.Component)) != len(declarations) {
		return fmt.Errorf("borrow_sql_row fresh authoring declarations differ")
	}
	workspace, err := (xmodule.LocalWorkspace{BaseDir: source.BaseDir()}).Resolve(ctx)
	if err != nil {
		return err
	}
	for _, declaration := range declarations {
		if err := declaration.Validate(); err != nil {
			return err
		}
		if declaration.Package != result.Component.TypeContext.PackagePath {
			return fmt.Errorf("borrow_sql_row cross-package generated borrowing is unsupported")
		}
		ownerPath := filepath.Join(source.BaseDir(), filepath.FromSlash(declaration.OwnerURI))
		if filepath.Clean(ownerPath) == filepath.Clean(source.Path) || strings.TrimSuffix(filepath.Base(ownerPath), filepath.Ext(ownerPath)) != declaration.OwnerName {
			return fmt.Errorf("borrow_sql_row owner must identify a different exact Source.Name and resource")
		}
		var ownerBytes []byte
		for _, f := range files {
			if f.Path == ownerPath {
				ownerBytes = f.Bytes
			}
		}
		if ownerBytes == nil {
			return fmt.Errorf("borrow_sql_row owner DQL resource is missing")
		}
		ownerSource := *source
		ownerSource.Name = declaration.OwnerName
		ownerRelative, err := filepath.Rel(source.BaseDir(), filepath.Dir(ownerPath))
		if err != nil {
			return err
		}
		if ownerRelative != "." {
			ownerSource.Scope = strings.TrimSuffix(source.Scope, "/") + "/" + filepath.ToSlash(ownerRelative)
		}
		ownerSource.Path = ownerPath
		ownerSource.Text = string(ownerBytes)
		freshOwner, err := freshBorrowedSource(ctx, &ownerSource, string(ownerBytes), files)
		if err != nil {
			return err
		}
		if len(authoredBorrowedSQLRows(freshOwner.Component)) != 0 {
			return fmt.Errorf("borrow_sql_row borrowed owners/cyclic dependencies are unsupported")
		}
		if freshOwner.Component.TypeContext == nil || freshOwner.Component.TypeContext.PackagePath != declaration.Package {
			return fmt.Errorf("borrow_sql_row owner canonical package differs")
		}
		owner, ownerGraph, err := resolveBorrowedBodySlot(freshOwner.Component, declaration.OwnerBodyPath)
		if err != nil {
			return err
		}
		if owner.Auxiliary {
			return fmt.Errorf("borrow_sql_row initial authority requires a physical SQL-generated leaf owner")
		}
		borrower, borrowerGraph, err := resolveBorrowedBodySlot(freshBorrower.Component, declaration.BodyPath)
		if err != nil {
			return err
		}
		actualSlot, actualGraph, err := resolveBorrowedBodySlot(result.Component, declaration.BodyPath)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(actualGraph, borrowerGraph) {
			return fmt.Errorf("borrow_sql_row package overlay changed the canonical body slot")
		}
		if actualSlot.Dest != "" || borrower.Dest != "" {
			return fmt.Errorf("borrow_sql_row borrowed slot cannot declare dest")
		}
		if owner.TypeName != declaration.Type || (borrower.TypeName != "" && borrower.TypeName != declaration.Type) || (actualSlot.TypeName != "" && actualSlot.TypeName != declaration.Type) {
			return fmt.Errorf("borrow_sql_row explicit canonical row type differs")
		}
		if err := column.ApplyWriterMetadata(freshOwner.Component); err != nil {
			return err
		}
		if err := column.ApplyWriterMetadata(freshBorrower.Component); err != nil {
			return err
		}
		expected, err := borrowedLeafContract(ctx, freshOwner, owner, declaration)
		if err != nil {
			return err
		}
		wanted, err := borrowedLeafContract(ctx, freshBorrower, borrower, declaration)
		if err != nil {
			return err
		}
		if err := gen.CompareBorrowedLeafContracts(expected, wanted); err != nil {
			return err
		}
		pkg, err := loadAvailablePackage(ctx, workspace, declaration.Package)
		if err != nil {
			return fmt.Errorf("borrow_sql_row owner descriptor unavailable; generate owner first: %w", err)
		}
		if pkg == nil {
			return fmt.Errorf("borrow_sql_row owner descriptor is missing; generate owner first")
		}
		if source.Types == nil {
			source.Types = typecatalog.NewCatalog()
		}
		if err := source.Types.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
			return err
		}
		resolver, err := typecatalog.NewResolver(source.Types, typecatalog.TranscribeAuthority, result.TypeContext)
		if err != nil {
			return err
		}
		row, err := resolver.Descriptor(declaration.Package + "." + declaration.Type)
		if err != nil {
			return err
		}
		marker, err := resolver.Descriptor(declaration.Package + "." + declaration.Type + "Has")
		if err != nil {
			return err
		}
		if err := gen.ValidateBorrowedDescriptor(expected, row, marker); err != nil {
			return err
		}
		location, err := workspace.Package(declaration.Package)
		if err != nil {
			return err
		}
		proof := &gen.BorrowedRowAuthority{Declaration: declaration, BorrowerSource: source.Path, BorrowerScope: source.Scope, BorrowerName: source.Name, BorrowerGraph: borrowerGraph, OwnerSource: ownerPath, OwnerScope: ownerSource.Scope, OwnerName: ownerSource.Name, OwnerGraph: ownerGraph, OwnerDirectory: location.Dir, Expected: expected, Files: append([]gen.BorrowedAuthorityFile(nil), files...)}
		var holderFiles []xmodule.File
		ownerEntries, err := os.ReadDir(location.Dir)
		if err != nil {
			return err
		}
		for _, entry := range ownerEntries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") {
				holderFiles = append(holderFiles, xmodule.File{Path: filepath.Join(location.Dir, entry.Name()), Dir: location.Dir, ImportPath: declaration.Package})
			}
		}
		routes, err := (bootstrap.PackageDiscovery{}).DiscoverFiles(holderFiles)
		if err != nil {
			return err
		}
		matches := 0
		ownerMutation := ""
		matchedRoutes := map[int]bool{}
		for _, holder := range routes {
			if holder.Tag.Name != ownerSource.Name {
				continue
			}
			index := -1
			for i, route := range freshOwner.Component.Routes {
				if route != nil && route.Path == holder.Tag.Path && strings.EqualFold(route.Method, holder.Tag.Method) {
					if index >= 0 {
						return fmt.Errorf("borrow_sql_row owner route identity is ambiguous")
					}
					index = i
				}
			}
			if index < 0 || matchedRoutes[index] || holder.PackagePath != declaration.Package || holder.Tag.Connector != borrowedConnector(freshOwner.Component, owner) {
				return fmt.Errorf("borrow_sql_row actual owner holder differs from exact fresh source identity")
			}
			route := freshOwner.Component.Routes[index]
			if route.RequestBodyMode != holder.Tag.RequestBodyMode || route.Internal != holder.Tag.Internal || route.APIKeyHeader != holder.Tag.APIKeyHeader || route.APIKeyValue != holder.Tag.APIKeyValue || (len(route.MCP) != len(holder.Tag.MCP) || len(route.MCP) > 0 && !reflect.DeepEqual(route.MCP, holder.Tag.MCP)) {
				return fmt.Errorf("borrow_sql_row actual owner route metadata differs from fresh source")
			}
			mutation := holder.Tag.Settings.Mutation
			if mutation != "post" && mutation != "put" && mutation != "patch" {
				return fmt.Errorf("borrow_sql_row owner has no native physical write operation")
			}
			if ownerMutation != "" && ownerMutation != mutation {
				return fmt.Errorf("borrow_sql_row owner mutation metadata is ambiguous")
			}
			ownerMutation = mutation
			matchedRoutes[index] = true
			matches++
		}
		if matches != len(freshOwner.Component.Routes) {
			return fmt.Errorf("borrow_sql_row complete owner component holder is missing/ambiguous: %d holders for %d canonical routes", matches, len(freshOwner.Component.Routes))
		}
		helperPackage, err := existingPrimaryPackageName(location.Dir)
		if err != nil {
			return err
		}
		helperInput := gen.Input{Component: freshOwner.Component.Clone(), Resources: freshOwner.Source.Resources, TargetPackage: declaration.Package, PackageName: helperPackage, SQLResources: true, TypeResolver: freshOwner.TypeResolver}
		// Native generation resolves the detached input before deriving Current
		// views. Use the same stage on captured resources so helper/resource
		// expectations match stock emission without changing authored provenance.
		if err := resolveComponentSources(helperInput.Component, freshOwner.Source.Resources); err != nil {
			return err
		}
		initialHelpers, err := gen.New(helperInput).Plan()
		if err != nil {
			return err
		}
		derivedHelpers, err := (&handlercompiler.Compiler{}).BuildInput(handlercompiler.Request{Component: helperInput.Component, Operation: WriteOperation(ownerMutation)}, initialHelpers.RootViewType)
		if err != nil {
			return err
		}
		helperCopy := *freshOwner
		helperCopy.Component, helperCopy.ViewBindings = derivedHelpers.Component, derivedHelpers.ViewBindings
		helperDeclarations, err := newDeclarationCompiler(helperCopy.Component, nil).compile()
		if err != nil {
			return err
		}
		helperInput.Declarations = helperDeclarations.generation
		helperInput.Component, helperInput.ViewBindings = derivedHelpers.Component, derivedHelpers.ViewBindings
		helperOptions := Options{Contracts: ContractsAuto, Handler: HandlerOptions{Target: HandlerGo, Operation: WriteOperation(ownerMutation), Input: derivedHelpers.Input, Output: derivedHelpers.Output, Currents: derivedHelpers.Currents, Go: GoHandlerOptions{Execution: GoExecutionMutation}}}
		helperGeneration := newHandlerGeneration(&helperCopy, &helperInput, helperOptions)
		helperGeneration.directory = location.Dir
		if err := helperGeneration.prepare(); err != nil {
			return fmt.Errorf("borrow_sql_row native owner helper derivation: %w", err)
		}
		proof.Helpers, err = gen.BorrowedNativeHelpers(helperInput.EntitySupport, declaration.Type)
		if err != nil {
			return err
		}
		validatedArtifacts, err := gen.BorrowedNativeOwnerArtifacts(helperInput, location.Dir)
		if err != nil {
			return err
		}
		for _, file := range validatedArtifacts {
			if err = gen.RetainBorrowedSeal(proof, file); err != nil {
				return err
			}
		}
		if err := sealBorrowedOwnerFiles(proof); err != nil {
			return err
		}
		proof.OwnerOperation = ownerMutation
		ownerCopy, borrowerCopy := ownerSource, *source
		frozen := append([]gen.BorrowedAuthorityFile(nil), files...)
		proof.ValidateSchema = func() error {
			currentOwner, err := freshBorrowedSource(ctx, &ownerCopy, ownerCopy.Text, frozen)
			if err != nil {
				return err
			}
			currentBorrower, err := freshBorrowedSource(ctx, &borrowerCopy, borrowerCopy.Text, frozen)
			if err != nil {
				return err
			}
			o, _, err := resolveBorrowedBodySlot(currentOwner.Component, declaration.OwnerBodyPath)
			if err != nil {
				return err
			}
			b, _, err := resolveBorrowedBodySlot(currentBorrower.Component, declaration.BodyPath)
			if err != nil {
				return err
			}
			if err := column.ApplyWriterMetadata(currentOwner.Component); err != nil {
				return err
			}
			if err := column.ApplyWriterMetadata(currentBorrower.Component); err != nil {
				return err
			}
			oe, err := borrowedLeafContract(ctx, currentOwner, o, declaration)
			if err != nil {
				return err
			}
			be, err := borrowedLeafContract(ctx, currentBorrower, b, declaration)
			if err != nil {
				return err
			}
			if err := gen.CompareBorrowedLeafContracts(expected, oe); err != nil {
				return err
			}
			return gen.CompareBorrowedLeafContracts(expected, be)
		}
		if err := proof.ValidateFiles(func(path string) string { return path }); err != nil {
			return err
		}
		actualSlot.TypeName = declaration.Type
		id, _ := actualSlot.Identity()
		if result.Views == nil {
			result.Views = gen.ViewReferences{}
		}
		if result.Views[id] != nil {
			return fmt.Errorf("borrow_sql_row slot already has separate linked authority")
		}
		result.Views[id] = &gen.ViewReference{DescriptorKey: row.Key(), Borrowed: proof}
		result.TypeResolver = resolver
	}
	return nil
}

func sealBorrowedOwnerFiles(proof *gen.BorrowedRowAuthority) error {
	entries, err := os.ReadDir(proof.OwnerDirectory)
	if err != nil {
		return err
	}
	found := map[string]int{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		path := filepath.Join(proof.OwnerDirectory, entry.Name())
		sealed, err := gen.SealBorrowedAuthorityFile(path)
		if err != nil {
			return err
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), path, sealed.Bytes, parser.ParseComments)
		if err != nil {
			return err
		}
		owned := strings.Contains(string(sealed.Bytes), "// Code generated by Datly for "+proof.OwnerName+"; DO NOT EDIT.")
		for _, decl := range parsed.Decls {
			g, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, s := range g.Specs {
				ts, ok := s.(*ast.TypeSpec)
				if !ok {
					continue
				}
				if ts.Name.Name == proof.Expected.Name || ts.Name.Name == proof.Expected.Name+"Has" {
					if !owned {
						return fmt.Errorf("borrow_sql_row row/Has declaration is not owned by exact generated component")
					}
					found[ts.Name.Name]++
					if ts.Name.Name == proof.Expected.Name {
						proof.RowFile = path
					} else {
						proof.HasFile = path
					}
				}
			}
		}
		if owned {
			for _, decl := range parsed.Decls {
				method, ok := decl.(*ast.FuncDecl)
				if !ok || method.Recv == nil || len(method.Recv.List) != 1 {
					continue
				}
				typ := method.Recv.List[0].Type
				if ptr, ok := typ.(*ast.StarExpr); ok {
					typ = ptr.X
				}
				receiver, ok := typ.(*ast.Ident)
				if !ok {
					continue
				}
				for i := range proof.Helpers {
					helper := &proof.Helpers[i]
					if receiver.Name == helper.Receiver && method.Name.Name == helper.Name {
						if helper.Path != "" {
							return fmt.Errorf("borrow_sql_row owner helper is ambiguous")
						}
						helper.Path = path
					}
				}
			}
			if err := gen.RetainBorrowedSeal(proof, sealed); err != nil {
				return err
			}
			if strings.Contains(string(sealed.Bytes), "//go:embed") {
				assets, err := packageasset.SourceEmbedFiles(proof.OwnerDirectory, sealed.Bytes)
				if err != nil {
					return err
				}
				for _, asset := range assets {
					resourceFile, err := gen.SealBorrowedAuthorityFile(filepath.Join(proof.OwnerDirectory, filepath.FromSlash(asset)))
					if err != nil {
						return err
					}
					if err := gen.RetainBorrowedSeal(proof, resourceFile); err != nil {
						return err
					}
				}
			}
		}
	}
	if found[proof.Expected.Name] != 1 || found[proof.Expected.Name+"Has"] != 1 {
		return fmt.Errorf("borrow_sql_row actual row/Has declaration is missing or ambiguous")
	}
	sort.Slice(proof.Files, func(i, j int) bool { return proof.Files[i].Path < proof.Files[j].Path })
	return nil
}

// applyBorrowedPhysicalFacts binds independently copied native evidence to the
// existing native emission; it never emits a replacement row or changes tags.
func applyBorrowedPhysicalFacts(contract *gen.BorrowedLeafContract, facts column.PhysicalSourceConstraints) error {
	if contract == nil || facts.Identity.Connector != contract.Connector || facts.Identity.Catalog != contract.Catalog || facts.Identity.Schema != contract.Schema || facts.Identity.Table != contract.Table {
		return fmt.Errorf("borrow_sql_row physical contract identity conflicts")
	}
	facts.Projection = append([]column.PhysicalProjectionFact(nil), facts.Projection...)
	columns := map[string]column.PhysicalColumnFact{}
	for _, c := range facts.NativeColumns {
		columns[c.Name] = c
	}
	fields := map[string]gen.Field{}
	for _, f := range contract.Fields {
		if _, exists := fields[f.Name]; exists {
			return fmt.Errorf("borrow_sql_row generated field is ambiguous")
		}
		fields[f.Name] = f
	}
	for i, p := range facts.Projection {
		f, ok := fields[typecatalog.FieldName(p.Projected)]
		if !ok {
			return fmt.Errorf("borrow_sql_row generated physical field is absent")
		}
		t := sqlio.ParseTag(reflect.StructTag(f.Tag))
		mapping := strings.Split(t.Column, "|")
		if t.Transient || len(mapping) == 0 || len(mapping) > 2 || mapping[0] != p.Physical {
			return fmt.Errorf("borrow_sql_row actual native field physical mapping conflicts")
		}
		if len(mapping) == 2 && mapping[1] != p.Result {
			return fmt.Errorf("borrow_sql_row actual native field result mapping conflicts")
		}
		if !strings.EqualFold(p.Result, p.Physical) && !strings.EqualFold(p.Result, f.Name) && (len(mapping) != 2 || mapping[1] != p.Result) {
			return fmt.Errorf("borrow_sql_row actual native field loses SQL result alias")
		}
		c := columns[p.Physical]
		primary := c.Primary
		unique := !primary && strings.EqualFold(c.Key, "UNI")
		auto := c.AutoIncrement != nil && *c.AutoIncrement
		if c.Default != nil {
			v := strings.ToLower(*c.Default)
			auto = auto || strings.Contains(v, "autoincrement") || strings.Contains(v, "auto_increment")
		}
		if t.PrimaryKey != primary || t.Autoincrement != auto || t.IsUnique != unique {
			return fmt.Errorf("borrow_sql_row actual native field key/autoincrement/unique metadata conflicts with physical facts")
		}
		references := 0
		for _, k := range facts.ForeignKeys {
			if k.Column != p.Physical {
				continue
			}
			references++
			schema := k.ReferenceSchema
			if facts.Product == "SQLite" && schema == "" {
				schema = facts.Identity.Schema
			}
			expectedDb := schema
			if schema == facts.Identity.Schema {
				expectedDb = ""
			}
			if t.RefDb != expectedDb || t.RefTable != k.ReferenceTable || t.RefColumn != k.ReferenceColumn {
				return fmt.Errorf("borrow_sql_row actual native field reference override conflicts with physical facts")
			}
		}
		if references == 0 && (t.RefDb != "" || t.RefTable != "" || t.RefColumn != "") {
			return fmt.Errorf("borrow_sql_row actual native field invents physical reference")
		}
		facts.Projection[i].Mapping = t.Column
	}
	serialized, err := json.Marshal(facts)
	if err != nil {
		return err
	}
	contract.Physical = serialized
	return nil
}
