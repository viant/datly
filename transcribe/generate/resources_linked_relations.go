package generate

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/tag"
	"github.com/viant/datly/typecatalog"
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
		if planned.Ownership != ViewLinked || planned.Package != r.input.TargetPackage {
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
		var visit func(*spec.View, *xshape.Type, string) error
		visit = func(view *spec.View, shape *xshape.Type, inherited string) error {
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
				if relation == nil || relation.View == nil {
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
				if err := visit(relation.View, xshape.New(resolved.Descriptor, r.types.Descriptor), nextNamespace); err != nil {
					return err
				}
			}
			return nil
		}
		if err := visit(view, shape, resources.Namespace); err != nil {
			return err
		}
	}
	return nil
}
