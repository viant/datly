package generate

import (
	"fmt"
	"go/ast"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
	sqlxio "github.com/viant/sqlx/io"
	xshape "github.com/viant/x/shape"
	"golang.org/x/mod/module"
)

// Linked row fields cannot be rewritten. Package their canonical relation SQL
// at the existing local URI, using the same resource ownership as generated rows.
func (r *planResolver) linkedRelationResources(resources *ResourcePlan, files map[string]string, retained []EmittedFile) error {
	retainedFiles := map[string]string{}
	for _, file := range retained {
		retainedFiles[file.Path] = file.Content
	}
	canonical, err := r.canonicalViewIndex()
	if err != nil {
		return err
	}
	for _, planned := range r.plan.Views {
		if planned.Ownership != ViewLinked {
			continue
		}
		view := canonical[planned.Identity]
		if view == nil {
			continue
		}
		descriptor, err := r.types.Descriptor(planned.DescriptorKey)
		if err != nil {
			return err
		}
		shape := xshape.New(descriptor, r.types.Descriptor)
		active := map[*spec.View]bool{}
		// Resolve the bound root once, only if new descriptor assets need it.
		// Canonical asset copies retain their existing namespace authority.
		var rootNamespace string
		var rootErr error
		rootResolved := false
		rootOwner := func() (string, error) {
			if !rootResolved {
				rootNamespace, rootErr = r.linkedDescriptorRootNamespace(resources, planned, view)
				rootResolved = true
			}
			return rootNamespace, rootErr
		}
		var visit func(*spec.View, *xshape.Type, string, func() (string, error)) error
		visit = func(view *spec.View, shape *xshape.Type, inherited string, descriptorOwner func() (string, error)) error {
			if active[view] {
				return fmt.Errorf("linked SQL relation graph has a cycle at %s", view.Name)
			}
			active[view] = true
			defer delete(active, view)
			fields, err := shape.Fields()
			if err != nil {
				return err
			}
			for _, relation := range view.Relations {
				// Derived relations are independent output holders, not fields
				// on the linked row. Their own planned view owns their resources.
				if relation == nil || relation.View == nil || relation.Kind == spec.RelationKindDerived {
					continue
				}
				name := typecatalog.FieldName(relation.Holder)
				if name == "" {
					name = typecatalog.FieldName(relation.Name)
				}
				var field *xshape.Field
				for i := range fields {
					if fields[i].Name == name && fields[i].Exported {
						field = &fields[i]
						break
					}
				}
				if field == nil {
					return fmt.Errorf("linked relation field %s.%s was not found", planned.Name, name)
				}
				nextNamespace := inherited
				source := tag.ParseSQL(field.Tag.Get(tag.SQLName))
				if relation.View.InMemory {
					source = nil
				}
				if source != nil && strings.TrimSpace(source.URI) != "" {
					path := strings.TrimSpace(source.URI)
					namespace, relative, qualified := strings.Cut(path, ":")
					if qualified {
						nextNamespace = namespace
						path = relative
					}
					if nextNamespace == resources.Namespace {
						if !fs.ValidPath(path) || module.CheckFilePath(path) != nil {
							return fmt.Errorf("linked relation %s has invalid SQL resource path %q", name, path)
						}
						if configured := r.plan.Generation.SQLFile(relation.View.CanonicalName(), ""); configured != "" && configured != path {
							return fmt.Errorf("linked relation %s SQL destination %q differs from uneditable URI %q", name, configured, path)
						}
						query := relation.View.RuntimeSource().Clone()
						if query == nil {
							return fmt.Errorf("linked relation %s SQL resource %q has no canonical source", name, path)
						}
						if query.SQL == "" && query.URI != "" && !strings.Contains(query.URI, ":") && r.input.Resources != nil {
							if _, found := r.input.Resources.Lookup(nextNamespace); found {
								query.URI = nextNamespace + ":" + query.URI
							}
						}
						if err := dsql.ResolveSource(relation.View.Name, query, r.input.Resources); err != nil {
							return err
						}
						if strings.TrimSpace(query.SQL) == "" {
							return fmt.Errorf("linked relation %s SQL resource %q has no canonical query", name, path)
						}
						query.SQL, err = r.linkedSQLResource(shape.Descriptor().PkgPath, path, query.SQL)
						if err != nil {
							return err
						}
						if previous, ok := files[path]; ok && previous != query.SQL {
							return fmt.Errorf("linked relation %s has conflicting SQL resource %q", name, path)
						}
						if resources.authoredSource() {
							previous, found := retainedFiles[path]
							if !found || previous != query.SQL {
								return fmt.Errorf("linked relation %s SQL resource %q differs from application-owned embedded filesystem", name, path)
							}
						}
						files[path] = query.SQL
					}
				}
				resolved, err := shape.ResolveField(name)
				if err != nil {
					return err
				}
				if resolved.Descriptor == nil {
					return fmt.Errorf("linked relation field %s.%s has no row type", planned.Name, name)
				}
				// New descriptor descendants inherit the actual bound root and
				// each enclosing field's effective URI, even across canonical edges.
				childOwner := func() (string, error) {
					namespace, err := descriptorOwner()
					if err != nil {
						return "", err
					}
					metadata, err := tag.ParseField(field.StructField())
					if err != nil {
						return "", err
					}
					uri := ""
					if metadata.View != nil {
						uri = metadata.View.URI
					}
					if metadata.SQL != nil && metadata.SQL.URI != "" {
						uri = metadata.SQL.URI
					}
					if explicit, _, qualified := strings.Cut(strings.TrimSpace(uri), ":"); qualified {
						namespace = explicit
					}
					return namespace, nil
				}
				if err := visit(relation.View, xshape.New(resolved.Descriptor, r.types.Descriptor), nextNamespace, childOwner); err != nil {
					return err
				}
			}
			// Runtime derives additional relations from the unchanged linked Go
			// row. Package their assets without constructing a second spec graph.
			covered := map[string]bool{}
			for _, relation := range view.Relations {
				if relation == nil || relation.View == nil || relation.Kind == spec.RelationKindDerived {
					continue
				}
				name := typecatalog.FieldName(relation.Holder)
				if name == "" {
					name = typecatalog.FieldName(relation.Name)
				}
				covered[name] = true
			}
			namespace := inherited
			for _, field := range fields {
				if field.Exported && !covered[field.Name] && strings.TrimSpace(field.Tag.Get(tag.RelationName)) != "" {
					namespace, err = descriptorOwner()
					if err != nil {
						return err
					}
					break
				}
			}
			return r.linkedDescriptorRelationAssets(resources, files, retainedFiles, shape, namespace, covered, map[string]bool{})
		}
		if err := visit(view, shape, resources.Namespace, rootOwner); err != nil {
			return err
		}
	}
	return nil
}

// linkedDescriptorRelationAssets follows only native on-tag row relations
// absent from the canonical graph. The selected descriptor owns each relative
// URI; neither a caller working directory nor a canonical SQL fallback does.
func (r *planResolver) linkedDescriptorRelationAssets(resources *ResourcePlan, files, retained map[string]string, shape *xshape.Type, inherited string, covered, active map[string]bool) error {
	descriptor := shape.Descriptor()
	if descriptor == nil {
		return fmt.Errorf("linked relation row descriptor is required")
	}
	identity := descriptor.PkgPath + "." + descriptor.Name
	if active[identity] {
		return fmt.Errorf("linked descriptor relation graph has a cycle at %s", identity)
	}
	active[identity] = true
	defer delete(active, identity)
	fields, err := shape.Fields()
	if err != nil {
		return err
	}
	for _, field := range fields {
		if !field.Exported || covered[field.Name] {
			continue
		}
		if strings.TrimSpace(field.Tag.Get(tag.RelationName)) == "" {
			continue
		}
		metadata, err := tag.ParseField(field.StructField())
		if err != nil {
			return fmt.Errorf("linked relation field %s.%s: %w", identity, field.Name, err)
		}
		if len(metadata.Relation) == 0 {
			continue
		}
		// Alias and named-wrapper descriptors can expose fields belonging to
		// another declaration. Only a concrete declaring row grants a new
		// package-relative resource; the canonical graph keeps its own authority.
		if source := descriptor.SynteticType; source != nil {
			if source.TypeSpec == nil || source.TypeSpec.Assign.IsValid() {
				return fmt.Errorf("linked relation row %s has unsupported alias ownership", identity)
			}
			if _, ok := source.TypeSpec.Type.(*ast.StructType); !ok {
				return fmt.Errorf("linked relation row %s has no concrete declaring struct", identity)
			}
		} else if descriptor.Type == nil || descriptor.Type.Name() == "" || descriptor.Type.PkgPath() != descriptor.PkgPath {
			return fmt.Errorf("linked relation row %s has no named declaring package", identity)
		}
		// Promoted source fields may belong to an embedded package. Their owning
		// declaration is not this descriptor, so they cannot grant a local URI.
		if len(field.Index) != 1 {
			return fmt.Errorf("linked relation field %s.%s has unsupported promoted ownership", identity, field.Name)
		}
		resolution, err := shape.ResolveField(field.Name)
		if err != nil {
			return err
		}
		if resolution.Descriptor == nil {
			return fmt.Errorf("linked relation field %s.%s has no row descriptor", identity, field.Name)
		}
		reference, err := (xshape.Resolver{}).Reference(resolution.Identity)
		if err != nil {
			return err
		}
		wrappers := reference.Wrappers
		valid := len(wrappers) == 0 || len(wrappers) == 1 && (wrappers[0].Kind == xshape.WrapperPointer || wrappers[0].Kind == xshape.WrapperSlice) || len(wrappers) == 2 && wrappers[0].Kind == xshape.WrapperSlice && wrappers[1].Kind == xshape.WrapperPointer
		child := xshape.New(resolution.Descriptor, r.types.Descriptor)
		structured, err := child.IsStruct()
		if err != nil {
			return err
		}
		if !valid || !structured {
			return fmt.Errorf("linked relation field %s.%s is not a supported native row holder", identity, field.Name)
		}
		scalar := sqlxio.ParseTag(field.Tag)
		if metadata.Codec != nil || metadata.Source != "" || !scalar.Transient && (scalar.Column != "" || scalar.Encoding != "") {
			return fmt.Errorf("linked relation field %s.%s also declares a scalar source or codec", identity, field.Name)
		}
		next := inherited
		// Match applyFieldViewTag/applyFieldSQLTag precedence without creating
		// views or parsing SQL. Inline SQL still retains a view URI's namespace.
		uri, inline := "", ""
		if metadata.View != nil {
			uri = strings.TrimSpace(metadata.View.URI)
		}
		if metadata.SQL != nil {
			if metadata.SQL.URI != "" {
				uri = strings.TrimSpace(metadata.SQL.URI)
			} else {
				inline = metadata.SQL.Text
			}
		}
		if uri != "" {
			path := uri
			namespace, relative, qualified := strings.Cut(path, ":")
			if qualified {
				next, path = namespace, relative
			}
			// Copy only this namespace's asset. Descendants still inherit the
			// actual namespace and may explicitly select this consumer again.
			if next == resources.Namespace && strings.TrimSpace(inline) == "" {
				if !fs.ValidPath(path) || module.CheckFilePath(path) != nil {
					return fmt.Errorf("linked relation %s has invalid SQL resource path %q", field.Name, path)
				}
				childName := field.Name
				if metadata.View != nil && metadata.View.Name != "" {
					childName = metadata.View.Name
				}
				if configured := r.plan.Generation.SQLFile(childName, ""); configured != "" && configured != path {
					return fmt.Errorf("linked relation %s SQL destination %q differs from uneditable URI %q", field.Name, configured, path)
				}
				body, err := r.linkedDescriptorSQLResource(descriptor.PkgPath, path)
				if err != nil {
					return err
				}
				if previous, ok := files[path]; ok && previous != body {
					return fmt.Errorf("linked relation %s has conflicting SQL resource %q", field.Name, path)
				}
				if resources.authoredSource() {
					previous, found := retained[path]
					if !found || previous != body {
						return fmt.Errorf("linked relation %s SQL resource %q differs from application-owned embedded filesystem", field.Name, path)
					}
				}
				files[path] = body
			}
		}
		if err := r.linkedDescriptorRelationAssets(resources, files, retained, child, next, nil, active); err != nil {
			return err
		}
	}
	return nil
}

func (r *planResolver) linkedDescriptorSQLResource(pkg, path string) (string, error) {
	if r.input.ProjectRoot == "" || pkg == "" {
		return "", fmt.Errorf("linked descriptor SQL resource %s:%s requires source package ownership", pkg, path)
	}
	authority, err := typecatalog.NewDestinationAuthority(r.input.ProjectRoot)
	if err != nil {
		return "", err
	}
	destination, err := authority.Resolve(pkg, "")
	if err != nil {
		return "", err
	}
	// Resolve's relative destination syntax is for generation, not authority to
	// reinterpret an unresolved descriptor package as the consumer directory.
	if destination.ImportPath != pkg {
		return "", fmt.Errorf("linked descriptor SQL package %q has no exact project module ownership", pkg)
	}
	root, err := os.OpenRoot(filepath.Join(r.input.ProjectRoot, destination.Directory))
	if err != nil {
		return "", err
	}
	defer root.Close()
	info, err := root.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("linked descriptor SQL resource %s:%s is not a regular file", pkg, path)
	}
	body, err := fs.ReadFile(root.FS(), path)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

// linkedDescriptorRootNamespace consumes the exact existing view binding and
// final contract field metadata. It never derives a namespace from row names.
func (r *planResolver) linkedDescriptorRootNamespace(resources *ResourcePlan, planned ViewPlan, view *spec.View) (string, error) {
	namespace := resources.Namespace
	selected := 0
	use := func(contract ContractPlan, field Field) error {
		raw := reflect.StructTag(field.Tag)
		if contract.Ownership == ContractLinked {
			descriptor, err := r.types.Descriptor(contract.DescriptorKey)
			if err != nil {
				return err
			}
			fields, err := xshape.New(descriptor, r.types.Descriptor).Fields()
			if err != nil {
				return err
			}
			found := false
			for _, actual := range fields {
				if actual.Exported && actual.Name == field.Name {
					if found {
						return fmt.Errorf("ambiguous linked resource binding %s", field.Name)
					}
					raw = actual.Tag
					found = true
				}
			}
			if !found {
				return fmt.Errorf("linked resource binding field %s was not found", field.Name)
			}
		}
		metadata, err := tag.ParseField(reflect.StructField{Name: field.Name, Tag: raw})
		if err != nil {
			return err
		}
		uri := ""
		if metadata.View != nil {
			uri = metadata.View.URI
		}
		if metadata.SQL != nil && metadata.SQL.URI != "" {
			uri = metadata.SQL.URI
		}
		selected++
		if selected > 1 {
			return fmt.Errorf("linked view %s has ambiguous effective SQL resource bindings", planned.Identity)
		}
		if explicit, _, qualified := strings.Cut(strings.TrimSpace(uri), ":"); qualified {
			namespace = explicit
		}
		return nil
	}
	for _, parameter := range spec.EffectiveParameters(r.input.Component.Parameters) {
		if parameter == nil || r.input.ViewBindings[parameter.Identity()] != planned.Identity {
			continue
		}
		field, ok := r.plan.Input.Field(typecatalog.FieldName(parameter.Name))
		if !ok {
			return "", fmt.Errorf("linked view %s has no exact input resource binding", planned.Identity)
		}
		if err := use(r.plan.Input, field); err != nil {
			return "", err
		}
	}
	if selected != 0 {
		return namespace, nil
	}
	if view == r.input.Component.RootView {
		for _, field := range r.plan.Output.Fields {
			metadata, err := tag.ParseField(reflect.StructField{Name: field.Name, Tag: reflect.StructTag(field.Tag)})
			if err != nil {
				return "", err
			}
			if metadata.Binding == nil || metadata.Binding.Location.Kind != "output" || metadata.Binding.Location.In != "view" {
				continue
			}
			canonical, err := r.plan.CanonicalType(r.plan.Output.Package, field.Type)
			if err != nil {
				return "", err
			}
			base, err := (xshape.Resolver{}).Named(canonical)
			if err != nil {
				return "", err
			}
			descriptor, err := r.types.Descriptor(base)
			if err != nil {
				return "", err
			}
			if descriptor == nil || descriptor.Key() != planned.DescriptorKey {
				continue
			}
			if err := use(r.plan.Output, field); err != nil {
				return "", err
			}
		}
	}
	if selected != 0 {
		return namespace, nil
	}
	if source := view.RuntimeSource(); source != nil {
		if explicit, _, qualified := strings.Cut(strings.TrimSpace(source.URI), ":"); qualified {
			// With no published contract fields, the supplied compiled view is the
			// runtime source authority. A partially matched contract must fail closed.
			if len(r.plan.Input.Fields) != 0 || len(r.plan.Output.Fields) != 0 {
				return "", fmt.Errorf("linked view %s has no exact effective resource binding", planned.Identity)
			}
			namespace = explicit
		}
	}
	return namespace, nil
}
