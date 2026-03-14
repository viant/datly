package transcriber

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	dqlparse "github.com/viant/datly/repository/shape/dql/parse"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/xgen"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type CodegenConfig struct {
	SourcePath string
	DQL        string
	ProjectDir string
	APIPrefix  string
	Hooks      *Hooks
}

type Hooks struct {
	Settings []SettingsHook
	Contract []ContractHook
	ViewSQL  []ViewSQLHook
}

type SettingsHook func(ctx context.Context, payload *SettingsPayload) error
type ContractHook func(ctx context.Context, payload *ContractPayload) error
type ViewSQLHook func(ctx context.Context, payload *ViewSQLPayload) error

type SettingsPayload struct {
	Config    *CodegenConfig
	Resource  *view.Resource
	Component *shapeLoad.Component
}

type ContractPayload struct {
	Config    *CodegenConfig
	Resource  *view.Resource
	Component *shapeLoad.Component
}

type ViewSQLPayload struct {
	Config    *CodegenConfig
	Resource  *view.Resource
	Component *shapeLoad.Component
	View      *view.View
	Name      string
	Root      bool
	SQL       string
}

func GenerateComponentCodegen(cfg CodegenConfig, resource *view.Resource, component *shapeLoad.Component) (*xgen.ComponentCodegenResult, error) {
	if component == nil || component.TypeContext == nil || resource == nil {
		return nil, nil
	}
	ctx := context.Background()
	projectDir := findProjectDir(cfg.SourcePath)
	if projectDir == "" {
		projectDir = strings.TrimSpace(cfg.ProjectDir)
	}
	if method, uri := ResolveRoute(cfg.SourcePath, cfg.DQL, cfg.APIPrefix); uri != "" {
		component.Method = method
		component.URI = uri
	}
	if err := ApplyHooks(ctx, &cfg, resource, component); err != nil {
		return nil, err
	}
	PrepareResourceForCodegen(resource, component)
	codegen := &xgen.ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  component.TypeContext,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}
	if pkgPath, pkgDir, pkgName := ResolveTypeOutput(projectDir, component.TypeContext.PackagePath); pkgPath != "" {
		codegen.PackagePath = pkgPath
		codegen.PackageDir = pkgDir
		codegen.PackageName = pkgName
	}
	return codegen.Generate()
}

func ApplyHooks(ctx context.Context, cfg *CodegenConfig, resource *view.Resource, component *shapeLoad.Component) error {
	if component == nil || resource == nil || cfg == nil {
		return nil
	}
	if method, uri := ResolveRoute(cfg.SourcePath, cfg.DQL, cfg.APIPrefix); uri != "" {
		component.Method = method
		component.URI = uri
	}
	if err := applySettingsHooks(ctx, cfg, resource, component); err != nil {
		return err
	}
	if err := applyContractHooks(ctx, cfg, resource, component); err != nil {
		return err
	}
	if err := applyViewSQLHooks(ctx, cfg, resource, component); err != nil {
		return err
	}
	return nil
}

func PrepareResourceForCodegen(resource *view.Resource, component *shapeLoad.Component) {
	if resource == nil || component == nil {
		return
	}
	rootView := ""
	if component != nil {
		rootView = strings.TrimSpace(component.RootView)
	}
	root := lookupNamedView(resource, rootView)
	if root == nil && len(resource.Views) > 0 {
		root = resource.Views[0]
	}
	if root != nil && root.Template != nil {
		preserveTemplateParameters(root, component.InputParameters())
		preserveTemplateParameters(root, resource.Parameters)
		if len(root.Template.Parameters) > 0 {
			root.Template.UseParameterStateType = true
		}
	}
	for _, item := range resource.Views {
		if item == nil || item.Template == nil {
			continue
		}
		if item.Template.DeclaredParametersOnly {
			item.Template.Parameters = append(state.Parameters{}, resource.Parameters.UsedBy(item.Template.Source)...)
		} else {
			preserveTemplateParameters(item, resource.Parameters.UsedBy(item.Template.Source))
			preserveTemplateParameters(item, dependentTemplateParameters(item.Template.Parameters, resource.Parameters))
		}
		if len(item.Template.Parameters) > 0 {
			item.Template.UseParameterStateType = true
		}
	}
}

func ResolveRoute(sourcePath, dql, apiPrefix string) (string, string) {
	if parsed, err := dqlparse.New().Parse(dql); err == nil && parsed != nil && parsed.Directives != nil && parsed.Directives.Route != nil {
		route := parsed.Directives.Route
		uri := strings.TrimSpace(route.URI)
		method := "GET"
		if len(route.Methods) > 0 && strings.TrimSpace(route.Methods[0]) != "" {
			method = strings.ToUpper(strings.TrimSpace(route.Methods[0]))
		}
		if uri != "" {
			return method, uri
		}
	}
	stem := strings.TrimSuffix(filepath.Base(sourcePath), filepath.Ext(sourcePath))
	uri := "/" + strings.Trim(stem, "/")
	if prefix := strings.TrimSpace(apiPrefix); prefix != "" {
		uri = strings.TrimRight(prefix, "/") + uri
	}
	return "GET", uri
}

func ResolveTypeOutput(projectDir, packagePath string) (string, string, string) {
	projectDir = strings.TrimSpace(projectDir)
	packagePath = strings.TrimSpace(packagePath)
	if projectDir == "" || packagePath == "" {
		return "", "", ""
	}
	modulePath, err := modulePath(filepath.Join(projectDir, "go.mod"))
	if err != nil || modulePath == "" {
		return "", "", ""
	}
	prefix := strings.TrimRight(modulePath, "/") + "/"
	if !strings.HasPrefix(packagePath, prefix) {
		return "", "", ""
	}
	rel := strings.TrimPrefix(packagePath, prefix)
	rel = sanitizeNamespace(rel)
	if rel == "" {
		return "", "", ""
	}
	pkgDir := filepath.Join(projectDir, filepath.FromSlash(rel))
	pkgName := filepath.Base(rel)
	return strings.TrimRight(modulePath, "/") + "/" + rel, pkgDir, pkgName
}

func findProjectDir(sourceAbsPath string) string {
	current := sourceAbsPath
	info, err := os.Stat(current)
	if err == nil && !info.IsDir() {
		current = filepath.Dir(current)
	}
	for current != "" && current != string(filepath.Separator) && current != "." {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	return ""
}

func modulePath(goModPath string) (string, error) {
	data, err := os.ReadFile(goModPath)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "module ") {
			return strings.TrimSpace(strings.TrimPrefix(line, "module ")), nil
		}
	}
	return "", fmt.Errorf("module path not found in %s", goModPath)
}

func sanitizeNamespace(namespace string) string {
	parts := strings.Split(strings.ReplaceAll(strings.TrimSpace(namespace), "\\", "/"), "/")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		switch part {
		case "":
			continue
		case "vendor":
			part = "vendorsrc"
		default:
			part = sanitizeNamespaceSegment(part)
		}
		parts[i] = part
	}
	return path.Join(parts...)
}

func sanitizeNamespaceSegment(segment string) string {
	var b strings.Builder
	for _, r := range segment {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '-':
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		return "generated"
	}
	ret := b.String()
	if ret[0] >= '0' && ret[0] <= '9' {
		return "p" + ret
	}
	return ret
}

func lookupNamedView(resource *view.Resource, name string) *view.View {
	if resource == nil || name == "" {
		return nil
	}
	if result, err := resource.View(name); err == nil && result != nil {
		return result
	}
	return nil
}

func preserveTemplateParameters(aView *view.View, params state.Parameters) {
	if aView == nil || aView.Template == nil || len(params) == 0 {
		return
	}
	if aView.Template.DeclaredParametersOnly {
		return
	}
	seen := map[string]bool{}
	for _, item := range aView.Template.Parameters {
		if item == nil || strings.TrimSpace(item.Name) == "" {
			continue
		}
		seen[strings.ToLower(strings.TrimSpace(item.Name))] = true
	}
	for _, param := range params {
		if param == nil || strings.TrimSpace(param.Name) == "" {
			continue
		}
		switch param.In.Kind {
		case state.KindOutput, state.KindMeta, state.KindAsync:
			continue
		}
		key := strings.ToLower(strings.TrimSpace(param.Name))
		if seen[key] {
			continue
		}
		aView.Template.Parameters = append(aView.Template.Parameters, param)
		seen[key] = true
	}
}

func dependentTemplateParameters(params state.Parameters, resourceParams state.Parameters) state.Parameters {
	if len(params) == 0 || len(resourceParams) == 0 {
		return nil
	}
	seen := map[string]bool{}
	result := make(state.Parameters, 0)
	for _, param := range params {
		if param == nil || param.In == nil {
			continue
		}
		if param.In.Kind != state.KindParam {
			continue
		}
		name := strings.TrimSpace(param.In.Name)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if seen[key] {
			continue
		}
		if dep := resourceParams.Lookup(name); dep != nil {
			result = append(result, dep)
			seen[key] = true
		}
	}
	return result
}

func applySettingsHooks(ctx context.Context, cfg *CodegenConfig, resource *view.Resource, component *shapeLoad.Component) error {
	if cfg == nil || cfg.Hooks == nil {
		return nil
	}
	for _, hook := range cfg.Hooks.Settings {
		if hook == nil {
			continue
		}
		if err := hook(ctx, &SettingsPayload{Config: cfg, Resource: resource, Component: component}); err != nil {
			return fmt.Errorf("settings hook: %w", err)
		}
	}
	return nil
}

func applyContractHooks(ctx context.Context, cfg *CodegenConfig, resource *view.Resource, component *shapeLoad.Component) error {
	if cfg == nil || cfg.Hooks == nil {
		return nil
	}
	for _, hook := range cfg.Hooks.Contract {
		if hook == nil {
			continue
		}
		if err := hook(ctx, &ContractPayload{Config: cfg, Resource: resource, Component: component}); err != nil {
			return fmt.Errorf("contract hook: %w", err)
		}
	}
	return nil
}

func applyViewSQLHooks(ctx context.Context, cfg *CodegenConfig, resource *view.Resource, component *shapeLoad.Component) error {
	if cfg == nil || cfg.Hooks == nil || resource == nil {
		return nil
	}
	rootView := strings.TrimSpace(component.RootView)
	for _, item := range resource.Views {
		if item == nil || item.Template == nil {
			continue
		}
		payload := &ViewSQLPayload{
			Config:    cfg,
			Resource:  resource,
			Component: component,
			View:      item,
			Name:      strings.TrimSpace(item.Name),
			Root:      strings.EqualFold(strings.TrimSpace(item.Name), rootView),
			SQL:       item.Template.Source,
		}
		for _, hook := range cfg.Hooks.ViewSQL {
			if hook == nil {
				continue
			}
			if err := hook(ctx, payload); err != nil {
				return fmt.Errorf("view sql hook: %w", err)
			}
		}
		item.Template.Source = payload.SQL
	}
	return nil
}
