package compiler

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	model "github.com/viant/x/syntetic/model"
)

func TestEntityHookCompilerGenericAndPromotedContracts(t *testing.T) {
	location := reflect.TypeOf(hookEntity{}).PkgPath()
	source := `package hooks
type Hook[T,P any]struct{}
func(*Hook[A,B])Init(c.Context,*A,h.EntityState[A,B])error{return nil}
func(*Hook[T,P])Validate(c.Context,*T,h.EntityState[T,P])error{return nil}
type Wrapper struct{Hook[e.hookEntity,h.NoParent]}
type Left struct{Hook[e.hookEntity,h.NoParent]}
type Right struct{Hook[e.hookEntity,h.NoParent]}
type Ambiguous struct{Left;Right}
type Blocked struct{Hook[e.hookEntity,h.NoParent];Init string}
`
	file, err := parser.ParseFile(token.NewFileSet(), "hooks.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	types := map[string]*x.Type{}
	for _, decl := range file.Decls {
		if group, ok := decl.(*ast.GenDecl); ok && group.Tok == token.TYPE {
			for _, item := range group.Specs {
				definition := item.(*ast.TypeSpec)
				types[definition.Name.Name] = &x.Type{Name: definition.Name.Name, PkgPath: "example.com/generic", SynteticType: &model.Type{Name: definition.Name.Name, PkgPath: "example.com/generic", TypeSpec: definition, Imports: map[string]*model.ImportRef{"c": {Path: "context"}, "h": {Path: "github.com/viant/xdatly/handler"}, "e": {Path: location}}}}
			}
		}
	}
	for _, decl := range file.Decls {
		if method, ok := decl.(*ast.FuncDecl); ok {
			receiver, err := (xshape.Resolver{}).Canonical(method.Recv.List[0].Type)
			if err != nil {
				t.Fatal(err)
			}
			reference, err := (xshape.Resolver{}).Reference(receiver)
			if err != nil {
				t.Fatal(err)
			}
			types[reference.BaseName].SynteticType.PtrMethodsAST = append(types[reference.BaseName].SynteticType.PtrMethodsAST, method)
		}
	}
	catalog := typecatalog.NewCatalog()
	for _, typ := range types {
		if err = catalog.Register(typecatalog.TypeOriginPackage, typ); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "app", Package: "example.com/generic"}}})
	if err != nil {
		t.Fatal(err)
	}
	compiler := EntityHookCompiler{Types: resolver}
	for _, test := range []struct {
		name, hook, parent string
		invalid            bool
	}{
		{"generic root", "app.Hook[" + location + ".hookEntity,github.com/viant/xdatly/handler.NoParent]", "", false},
		{"generic child", "app.Hook[" + location + ".hookEntity," + location + ".hookParent]", location + ".hookParent", false},
		{"wrong generic parent", "app.Hook[" + location + ".hookEntity," + location + ".hookParent]", "", true},
		{"promoted generic", "app.Wrapper", "", false},
		{"ambiguous diamond", "app.Ambiguous", "", true},
		{"field shadows method", "app.Blocked", "", true},
		{"wrong arity", "app.Hook[int]", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual, err := compiler.Compile(EntityHookRequest{Hook: test.hook, Entity: location + ".hookEntity", Parent: test.parent})
			if (err != nil) != test.invalid {
				t.Fatalf("Compile = %+v, %v", actual, err)
			}
			if !test.invalid && actual.Package != "example.com/generic" {
				t.Fatalf("package = %q", actual.Package)
			}
			if !test.invalid && strings.Contains(test.hook, "[") && !strings.Contains(actual.Name, "[") {
				t.Fatal("canonical specialization arguments lost")
			}
		})
	}
}
