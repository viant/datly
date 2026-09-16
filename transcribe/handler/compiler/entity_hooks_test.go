package compiler

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	model "github.com/viant/x/syntetic/model"
	xhandler "github.com/viant/xdatly/handler"
)

type hookEntity struct{}
type hookParent struct{}
type rootEntityHooks struct{}

func (*rootEntityHooks) Init(context.Context, *hookEntity, xhandler.LifecycleContext[hookEntity, xhandler.NoParent, hookOutput]) error {
	return nil
}
func (*rootEntityHooks) Validate(context.Context, *hookEntity, xhandler.LifecycleContext[hookEntity, xhandler.NoParent, hookOutput]) error {
	return nil
}

type childEntityHooks struct{}

func (childEntityHooks) Init(context.Context, *hookEntity, xhandler.LifecycleContext[hookEntity, hookParent, hookOutput]) error {
	return nil
}
func (*childEntityHooks) Validate(context.Context, *hookEntity, xhandler.LifecycleContext[hookEntity, hookParent, hookOutput]) error {
	return nil
}

func TestEntityHookCompilerCanonicalContracts(t *testing.T) {
	location := reflect.TypeOf(hookEntity{}).PkgPath()
	catalog := typecatalog.NewCatalog()
	for _, typ := range []reflect.Type{reflect.TypeOf(rootEntityHooks{}), reflect.TypeOf(childEntityHooks{})} {
		if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			t.Fatal(err)
		}
	}
	for _, item := range []struct{ path, parent, entity string }{{"example.com/one", "h.NoParent", "e.hookEntity"}, {"example.com/two", "e.hookParent", "e.hookEntity"}, {"example.com/wrong", "h.NoParent", "e.hookParent"}, {"example.com/imposter", "h.NoParent", "e.hookEntity"}} {
		file, err := parser.ParseFile(token.NewFileSet(), "hooks.go", "package hooks\nfunc(*Hooks)Init(ctx c.Context,current *"+item.entity+",state h.LifecycleContext["+item.entity+","+item.parent+",e.hookOutput])error{return nil}\nfunc(*Hooks)Validate(ctx c.Context,current *"+item.entity+",state h.LifecycleContext["+item.entity+","+item.parent+",e.hookOutput])error{return nil}", 0)
		if err != nil {
			t.Fatal(err)
		}
		descriptor := &x.Type{Name: "Hooks", PkgPath: item.path, SynteticType: &model.Type{Name: "Hooks", PkgPath: item.path, TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Hooks"), Type: &ast.StructType{Fields: &ast.FieldList{}}}, Imports: map[string]*model.ImportRef{"c": {Path: "context"}, "h": {Path: "github.com/viant/xdatly/handler"}, "e": {Path: location}}, PtrMethodsAST: []*ast.FuncDecl{file.Decls[0].(*ast.FuncDecl), file.Decls[1].(*ast.FuncDecl)}}}
		if item.path == "example.com/imposter" {
			descriptor.SynteticType.Imports["e"] = &model.ImportRef{Path: "example.com/other"}
		}
		if err = catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: location, Imports: []typecatalog.PackageImport{{Alias: "one", Package: "example.com/one"}, {Alias: "two", Package: "example.com/two"}}})
	if err != nil {
		t.Fatal(err)
	}
	compiler := EntityHookCompiler{Types: resolver}
	for _, test := range []struct {
		name, hook, parent string
		invalid            bool
		wantPackage        string
	}{
		{"linked root", "rootEntityHooks", "", false, location},
		{"linked child", "childEntityHooks", location + ".hookParent", false, location},
		{"incorrect linked parent", "childEntityHooks", "", true, ""},
		{"synthetic alias", "one.Hooks", "", false, "example.com/one"},
		{"synthetic full path", "example.com/two.Hooks", location + ".hookParent", false, "example.com/two"},
		{"same short name wrong parent", "two.Hooks", "", true, ""},
		{"wrong entity", "example.com/wrong.Hooks", "", true, ""},
		{"same entity short name wrong module", "example.com/imposter.Hooks", "", true, ""},
		{"unknown", "one.Absent", "", true, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual, err := compiler.Compile(EntityHookRequest{Hook: test.hook, Entity: location + ".hookEntity", Parent: test.parent, Output: location + ".hookOutput"})
			if (err != nil) != test.invalid {
				t.Fatalf("Compile = %+v, %v", actual, err)
			}
			if !test.invalid && actual.Package != test.wantPackage {
				t.Fatalf("hook package = %q", actual.Package)
			}
		})
	}
}
