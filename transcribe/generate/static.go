package generate

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/packageasset"
	"go/format"
	"io/fs"
	"strings"
)

func (r *planResolver) resolveStatic() (*Plan, error) {
	component := r.input.Component
	if component.RootView != nil || len(component.Views) > 0 || len(component.Parameters) > 0 || len(component.Routes) > 0 || component.Settings != nil || r.input.GoHandler != nil || r.input.VeltyHandler != nil || r.input.ContractHandler != nil || r.input.MutationHandler != nil {
		return nil, fmt.Errorf("static generation cannot include executable component metadata")
	}
	content := component.Static.Clone()
	if err := content.Validate(); err != nil {
		return nil, err
	}
	r.plan = &Plan{Package: r.input.TargetPackage, GoPackage: r.input.PackageName, ProjectRoot: r.input.ProjectRoot, ComponentName: r.input.Component.Name, Static: content, RouterDest: lowerSnake(r.input.Component.Name) + "_static.go"}
	var err error
	r.plan.Resources, err = r.prepareResources()
	return r.plan, err
}
func (r *planResolver) packageStatic(resources *ResourcePlan, files map[string]string) error {
	content := r.plan.Static
	if content.Namespace == "" {
		return fmt.Errorf("static generation requires static_resource with an explicit resource namespace")
	}
	source, ok := r.input.Resources.Lookup(content.Namespace)
	if !ok {
		return fmt.Errorf("static resource namespace %q is not registered", content.Namespace)
	}
	snapshot, err := (packageasset.Snapshotter{Source: source}).Folder(context.Background(), content.Root)
	if err != nil {
		return err
	}
	root := "datly_assets/static"
	err = fs.WalkDir(snapshot, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if strings.ContainsAny(name, "*?[]\\") {
			return fmt.Errorf("static filename %q cannot be embedded literally", name)
		}
		data, err := fs.ReadFile(snapshot, name)
		if err != nil {
			return err
		}
		files[root+"/"+name] = string(data)
		return nil
	})
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("static resource folder is empty")
	}
	content.Namespace, content.Root = resources.Namespace, root
	return nil
}
func (p *Plan) staticSource(packageName string) (string, error) {
	s := p.Static
	// Policy is authored metadata. Static DQL currently has no CORS override;
	// global standalone CORS applies, and linked/configured metadata may set it.
	if s.CORS != nil {
		return "", fmt.Errorf("static generation of a CORS override is not supported")
	}
	source := fmt.Sprintf("package %s\nimport \"github.com/viant/datly/spec\"\n// DatlyStaticContent returns detached metadata for standalone.Config.StaticContent.\nfunc DatlyStaticContent() *spec.StaticContent { return &spec.StaticContent{Path:%q, Namespace:%q, Root:%q, APIKeyHeader:%q, APIKeyValue:%q} }\n", packageName, s.Path, s.Namespace, s.Root, s.APIKeyHeader, s.APIKeyValue)
	data, err := format.Source([]byte(source))
	return string(data), err
}
