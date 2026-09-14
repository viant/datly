package build

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/format"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/x"
	loader "github.com/viant/x/loader/ast"
	xmodule "github.com/viant/x/module"
	"github.com/viant/x/shape"
	"github.com/viant/x/syntetic/model"
)

type linker struct {
	selection *xmodule.BuildSelection
	workspace *xmodule.Workspace
	packages  map[string]*model.Package
	types     map[string]bool
	imports   map[string]string
	factories map[string]string
	ctx       context.Context
}

func newLinker(selection *xmodule.BuildSelection) *linker {
	return &linker{selection: selection, workspace: selection.Workspace(), packages: map[string]*model.Package{}, types: map[string]bool{}, imports: map[string]string{}, factories: map[string]string{}}
}
func (l *linker) load(path string) (*model.Package, error) {
	if pkg := l.packages[path]; pkg != nil {
		return pkg, nil
	}
	loc, err := l.workspace.Package(path)
	if err != nil || loc == nil {
		return nil, err
	}
	relative, err := filepath.Rel(loc.Module.Dir, loc.Dir)
	if err != nil {
		return nil, err
	}
	pkg, err := loader.LoadPackageFS(l.ctx, l.workspace.SourceFS(loc.Module), filepath.ToSlash(relative))
	if err != nil {
		return nil, err
	}
	l.packages[path] = pkg
	return pkg, nil
}
func (l *linker) lookup(key string) (*x.Type, error) {
	ref, err := (shape.Resolver{}).Reference(key)
	if err != nil {
		return nil, err
	}
	pkg, err := l.load(ref.Qualifier)
	if err != nil {
		return nil, err
	}
	if pkg == nil {
		return nil, fmt.Errorf("source type %s is unavailable in Go build", key)
	}
	for _, typ := range pkg.Types {
		if typ.Name == ref.BaseName {
			return &x.Type{Name: typ.Name, PkgPath: pkg.PkgPath, SynteticType: typ}, nil
		}
	}
	return nil, fmt.Errorf("type %s is not declared in selected Go files", key)
}
func (l *linker) qualify(path, suggested string) string {
	if alias := l.imports[path]; alias != "" {
		return alias
	}
	alias := fmt.Sprintf("pkg%d", len(l.imports))
	l.imports[path] = alias
	return alias
}
func (l *linker) expression(key string) (string, error) {
	return (shape.Resolver{Rewriter: func(name string) (string, error) {
		ref, err := (shape.Resolver{}).Reference(name)
		if err != nil {
			return "", err
		}
		if ref.Qualifier == "" {
			return ref.Name, nil
		}
		return l.qualify(ref.Qualifier, "") + "." + ref.Name, nil
	}}).Rewrite(key)
}
func (l *linker) collect(key string) error {
	if l.types[key] {
		return nil
	}
	ref, err := (shape.Resolver{}).Reference(key)
	if err != nil {
		return fmt.Errorf("unlinkable type %s: %w", key, err)
	}
	if !ref.Exported() || len(ref.Wrappers) > 0 {
		return fmt.Errorf("unlinkable declaration %s: contracts and reachable named shapes must be exported", key)
	}
	l.types[key] = true
	loc, err := l.workspace.Package(ref.Qualifier)
	if err != nil {
		return err
	}
	if loc == nil {
		for _, pkg := range l.selection.Packages {
			if pkg.ImportPath == ref.Qualifier && pkg.Standard {
				return nil
			}
		}
		return fmt.Errorf("unlinkable type %s: package was not selected by Go", key)
	}
	resolved, err := (shape.Resolver{Lookup: l.lookup}).Resolve(key)
	if err != nil {
		return fmt.Errorf("unlinkable type %s: %w", key, err)
	}
	if resolved.Descriptor.SynteticType != nil && len(resolved.Descriptor.SynteticType.TypeParams) > 0 {
		return fmt.Errorf("unlinkable type %s: uninstantiated generic declaration", key)
	}
	refs, err := shape.New(resolved.Descriptor, l.lookup).References()
	if err != nil {
		return err
	}
	for _, child := range refs {
		if err := l.collect(child); err != nil {
			return err
		}
	}
	return nil
}
func (l *linker) factory(route *bootstrap.RouteSource, resolver shape.Resolver) error {
	if route.Tag.Handler == "" {
		return nil
	}
	key, err := resolver.Canonical(route.Tag.Handler)
	if err != nil {
		return err
	}

	ref, err := (shape.Resolver{}).Reference(key)
	if err != nil {
		return err
	}
	if !ref.Exported() || len(ref.Wrappers) > 0 || len(ref.Arguments) > 0 {
		return fmt.Errorf("unlinkable handler %s: expected exported non-generic factory", key)
	}
	pkg, err := l.load(ref.Qualifier)
	if err != nil {
		return err
	}
	if pkg == nil {
		return fmt.Errorf("unlinkable handler %s: package unavailable", key)
	}
	var factory *model.Function
	for _, fn := range pkg.Funcs {
		if fn.Name == ref.Name {
			factory = fn
			break
		}
	}
	if factory == nil {
		return fmt.Errorf("unlinkable handler %s: function not declared in selected files", key)
	}
	fn := factory.Decl.Type
	if len(factory.Type.Params) > 0 || factory.Type.Variadic || len(factory.Type.TypeParams) > 0 || len(factory.Type.Results) < 1 || len(factory.Type.Results) > 2 {
		return fmt.Errorf("unlinkable handler %s: expected zero-argument factory", key)
	}
	// The existing loader/model and shape resolver own signature syntax and identity.
	signature := shape.Resolver{Package: pkg.PkgPath, Imports: map[string]string{}}
	for _, file := range pkg.Files {
		if file.Name == factory.File {
			for _, imp := range file.Imports {
				alias := imp.Alias
				if alias == "" {
					for _, selected := range l.selection.Packages {
						if selected.ImportPath == imp.Path {
							alias = selected.Name
							break
						}
					}
				}
				signature.Imports[alias] = imp.Path
			}
		}
	}
	result, err := signature.Canonical(fn.Results.List[0].Type)
	if err != nil {
		return err
	}
	if len(factory.Type.Results) == 2 {
		second, err := signature.Canonical(fn.Results.List[len(fn.Results.List)-1].Type)
		if err != nil || second != "error" {
			return fmt.Errorf("unlinkable handler %s: second result must be error", key)
		}
	}
	value, err := l.expression(key)
	if err != nil {
		return err
	}
	bridge := value
	if result != "github.com/viant/datly/runtime/handler.TypedHandler" {
		generic, err := (shape.Resolver{}).Generic(result)
		if err != nil || len(generic.Arguments) != 2 || len(factory.Type.Results) != 1 {
			return fmt.Errorf("unlinkable handler %s: return Contract[I,O], Definition[I,O], or TypedHandler (optionally with error)", key)
		}
		adapter := ""
		switch generic.Qualifier + "." + generic.BaseName {
		case "github.com/viant/xdatly/handler.Contract":
			adapter = "custom"
		case "github.com/viant/xdatly/handler/mutation.Definition":
			adapter = "mutation"
		default:
			return fmt.Errorf("unlinkable handler %s: unsupported result %s", key, result)
		}
		alias := l.qualify("github.com/viant/datly/runtime/handler/"+adapter, adapter)
		contracts := make([]string, 0, 2)
		for _, contract := range []string{route.InputType, route.OutputType} {
			canonical, err := resolver.Canonical(contract)
			if err != nil {
				return err
			}
			expression, err := l.expression(canonical)
			if err != nil {
				return err
			}
			contracts = append(contracts, expression)
		}
		bridge = alias + ".Factory[" + strings.Join(contracts, ",") + "](" + value + ")"
	}
	if previous := l.factories[key]; previous != "" && previous != bridge {
		return fmt.Errorf("handler %s is referenced with conflicting component contracts", key)
	}
	l.factories[key] = bridge
	return nil
}
func (l *linker) generate(ctx context.Context, modulePath string) ([]byte, *Result, error) {
	l.ctx = ctx
	routes, err := (bootstrap.PackageDiscovery{Workspace: l.workspace, Include: []string{"..."}, Exclude: []string{modulePath + "/internal/datlylink", modulePath + "/cmd/datly"}}).Discover(ctx)
	if err != nil {
		return nil, nil, err
	}
	for _, route := range routes {
		if route.PackageName == "main" {
			return nil, nil, fmt.Errorf("unlinkable component %s: package main cannot be imported", route.SourceFile)
		}
		resolver := shape.Resolver{Package: route.PackagePath, Imports: map[string]string{}}
		for _, imp := range route.Imports {
			resolver.Imports[imp.Alias] = imp.Package
		}
		for _, name := range []string{route.InputType, route.OutputType} {
			key, err := resolver.Canonical(name)
			if err != nil {
				return nil, nil, err
			}
			if err = l.collect(key); err != nil {
				return nil, nil, fmt.Errorf("%s.%s: %w", route.HolderType, route.FieldName, err)
			}
		}
		if err = l.factory(route, resolver); err != nil {
			return nil, nil, err
		}
	}
	var body bytes.Buffer
	fmt.Fprintln(&body, "func Registry() (*x.Registry,error) { r:=x.NewRegistry()")
	keys := make([]string, 0, len(l.types))
	for key := range l.types {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		expr, err := l.expression(key)
		if err != nil {
			return nil, nil, err
		}
		fmt.Fprintf(&body, "r.Register(x.NewType(reflect.TypeFor[%s](),x.WithPkgPath(%q),x.WithName(%q)))\n", expr, l.typePackage(key), l.typeName(key))
	}
	keys = keys[:0]
	for key := range l.factories {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		ref, _ := (shape.Resolver{}).Reference(key)
		fmt.Fprintf(&body, "{f,err:=x.NewFunction(%q,%q,%s);if err!=nil{return nil,err};if err=r.RegisterFunctions(f);err!=nil{return nil,err}}\n", ref.Qualifier, ref.Name, l.factories[key])
	}
	fmt.Fprintln(&body, "return r,nil }")
	// Go source and resources retain existing source-backed runtime ownership.
	// Import holders even when all their contracts live in other packages. This
	// makes Go verify every discovered declaration, including metadata-only holders.
	for _, route := range routes {
		if l.imports[route.PackagePath] == "" {
			l.imports[route.PackagePath] = "_"
		}
	}
	snapshot, err := json.Marshal(l.selection)
	if err != nil {
		return nil, nil, err
	}
	fmt.Fprintf(&body, "func Workspace() *module.Workspace {var selection module.BuildSelection;if err:=json.Unmarshal([]byte(%q),&selection);err!=nil{panic(err)};return selection.Workspace()}\n", snapshot)
	var out bytes.Buffer
	fmt.Fprintln(&out, "// Code generated by datly project build. DO NOT EDIT.\npackage datlylink\nimport (\"encoding/json\";\"github.com/viant/x\";\"github.com/viant/x/module\"")
	if len(l.types) > 0 {
		fmt.Fprintln(&out, "\"reflect\"")
	}
	paths := make([]string, 0, len(l.imports))
	for path := range l.imports {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		fmt.Fprintf(&out, "%s %q\n", l.imports[path], path)
	}
	fmt.Fprintln(&out, ")")
	out.Write(body.Bytes())
	formatted, err := format.Source(out.Bytes())
	if err != nil {
		return nil, nil, err
	}
	return formatted, &Result{Components: len(routes), Types: len(l.types), Factories: len(l.factories)}, nil
}
func (l *linker) typePackage(key string) string {
	ref, _ := (shape.Resolver{}).Reference(key)
	return ref.Qualifier
}
func (l *linker) typeName(key string) string {
	ref, _ := (shape.Resolver{}).Reference(key)
	return strings.TrimPrefix(key, ref.Qualifier+".")
}
