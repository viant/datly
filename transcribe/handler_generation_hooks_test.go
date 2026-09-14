package transcribe

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	model "github.com/viant/x/syntetic/model"
)

func TestHandlerGenerationValidatesEntityHooksAfterEntityAuthority(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	for _, hook := range []struct{ name, entity, parent string }{{"RootHooks", "Order", "h.NoParent"}, {"ChildHooks", "Item", "e.Order"}, {"WrongHooks", "Item", "h.NoParent"}} {
		file, err := parser.ParseFile(token.NewFileSet(), "hooks.go", "package hooks\nfunc(*"+hook.name+")Init(c.Context,*e."+hook.entity+",h.EntityState[e."+hook.entity+","+hook.parent+"])error{return nil}\nfunc(*"+hook.name+")Validate(c.Context,*e."+hook.entity+",h.EntityState[e."+hook.entity+","+hook.parent+"])error{return nil}", 0)
		if err != nil {
			t.Fatal(err)
		}
		descriptor := &x.Type{Name: hook.name, PkgPath: "example.com/hooks", SynteticType: &model.Type{Name: hook.name, PkgPath: "example.com/hooks", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent(hook.name), Type: &ast.StructType{Fields: &ast.FieldList{}}}, Imports: map[string]*model.ImportRef{"c": {Path: "context"}, "h": {Path: "github.com/viant/xdatly/handler"}, "e": {Path: "example.com/generated"}}, PtrMethodsAST: []*ast.FuncDecl{file.Decls[0].(*ast.FuncDecl), file.Decls[1].(*ast.FuncDecl)}}}
		if err = catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "app", Package: "example.com/hooks"}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, childHook string
		invalid         bool
	}{{"typed parent", "app.ChildHooks", false}, {"incorrect parent", "app.WrongHooks", true}, {"unknown", "app.Missing", true}} {
		t.Run(test.name, func(t *testing.T) {
			childView := &spec.View{Name: "Items", EntityHooks: test.childHook}
			rootView := &spec.View{Name: "Orders", EntityHooks: "app.RootHooks", Relations: []*spec.Relation{{Holder: "Items", View: childView}}}
			rootID, _ := rootView.Identity()
			childID, _ := childView.Identity()
			child := &plan.RecordPlan{Identity: childID, InputPath: plan.FieldPath{"Input", "Orders", "Items"}, Cardinality: spec.CardinalityMany, Entity: &plan.EntityPlan{Owned: true}}
			root := &plan.RecordPlan{Identity: rootID, InputPath: plan.FieldPath{"Input", "Orders"}, Cardinality: spec.CardinalityMany, Entity: &plan.EntityPlan{Owned: true}, Relations: []*plan.RelationPlan{{FieldPath: plan.FieldPath{"Items"}, Child: child}}}
			semantic := &plan.Plan{Root: root}
			generated := &gen.Plan{Views: []gen.ViewPlan{{Identity: rootID, Name: "Order", Type: "Order", Ownership: gen.ViewGenerated}, {Identity: childID, Name: "Item", Type: "Item", Ownership: gen.ViewGenerated}}}
			generation := newHandlerGeneration(&Result{Component: &spec.Component{RootView: rootView}}, &gen.Input{TypeResolver: resolver, TargetPackage: "example.com/generated"}, Options{})
			err := generation.compileEntityHooks(semantic, generated, []handlergo.RecordType{{Identity: rootID, Path: root.InputPath, Value: "[]*Order"}, {Identity: childID, Path: child.InputPath, Value: "[]*Item"}})
			if (err != nil) != test.invalid {
				t.Fatalf("hook generation = %v", err)
			}
			if !test.invalid && (root.Entity.Hooks.Package != "example.com/hooks" || root.Entity.Hooks.Name != "RootHooks" || child.Entity.Hooks.Name != "ChildHooks") {
				t.Fatalf("canonical hooks root=%+v child=%+v", root.Entity.Hooks, child.Entity.Hooks)
			}
			if rootView.EntityHooks != "app.RootHooks" || childView.EntityHooks != test.childHook {
				t.Fatal("authored component metadata changed")
			}
		})
	}
}
