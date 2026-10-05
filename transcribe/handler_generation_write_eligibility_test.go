package transcribe

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	model "github.com/viant/x/syntetic/model"
)

func TestHandlerGenerationWriteEligibilityCanonicalAdmission(t *testing.T) {
	for _, tc := range []struct {
		name    string
		method  bool
		mutate  func(*plan.RecordPlan)
		invalid bool
	}{
		{name: "insert update root", method: true},
		{name: "insert only root", method: true, mutate: func(r *plan.RecordPlan) { r.Write.Allowed = []plan.Action{plan.ActionInsert} }},
		{name: "update only root", method: true, mutate: func(r *plan.RecordPlan) { r.Write.Allowed = []plan.Action{plan.ActionUpdate} }},
		{name: "read only auxiliary relation", method: true, mutate: func(r *plan.RecordPlan) { r.Relations[0].Child.Auxiliary = true }},
		{name: "writable child", method: true, mutate: func(r *plan.RecordPlan) { r.Relations[0].Child.Auxiliary = false }, invalid: true},
		{name: "recursive writable descendants", method: true, mutate: func(r *plan.RecordPlan) {
			r.SelfRelations = []plan.SelfRelationPlan{{FieldPath: plan.FieldPath{"Children"}}}
		}, invalid: true},
		{name: "writable grandchild behind auxiliary", method: true, mutate: func(r *plan.RecordPlan) {
			r.Relations[0].Child.Relations = []*plan.RelationPlan{{Child: &plan.RecordPlan{}}}
		}, invalid: true},
		{name: "delete enabled", method: true, mutate: func(r *plan.RecordPlan) { r.Write.Allowed = append(r.Write.Allowed, plan.ActionDelete) }, invalid: true},
		{name: "delete decision", method: true, mutate: func(r *plan.RecordPlan) { r.Write.Existing = plan.ActionDelete }, invalid: true},
		{name: "delete marker", method: true, mutate: func(r *plan.RecordPlan) { r.Write.DeleteMarker.Field = "Deleted" }, invalid: true},
		{name: "unknown action", method: true, mutate: func(r *plan.RecordPlan) { r.Write.Allowed = []plan.Action{"unknown"} }, invalid: true},
		{name: "missing policy", method: true, mutate: func(r *plan.RecordPlan) { r.Write.Allowed = nil }, invalid: true},
		{name: "auxiliary root", method: true, mutate: func(r *plan.RecordPlan) { r.Auxiliary = true; r.Relations = nil }, invalid: true},
		{name: "auxiliary root above writable child", method: true, mutate: func(r *plan.RecordPlan) { r.Auxiliary = true; r.Relations[0].Child.Auxiliary = false }, invalid: true},
		{name: "no eligibility method retains delete support", mutate: func(r *plan.RecordPlan) { r.Write.Allowed = append(r.Write.Allowed, plan.ActionDelete) }},
		{name: "no eligibility method retains writable child support", mutate: func(r *plan.RecordPlan) { r.Relations[0].Child.Auxiliary = false }},
		{name: "no eligibility method retains unknown policy behavior", mutate: func(r *plan.RecordPlan) { r.Write.Allowed = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			source := `package hooks
func(*Rules)Init(c.Context,*e.Order,h.LifecycleContext[e.Order,h.NoParent,e.Output])error{return nil}
func(*Rules)Validate(c.Context,*e.Order,h.LifecycleContext[e.Order,h.NoParent,e.Output])error{return nil}
`
			if tc.method {
				source += `func(*Rules)WriteEligible(c.Context,*e.Order,h.LifecycleContext[e.Order,h.NoParent,e.Output],h.WriteAction)(bool,error){return true,nil}`
			}
			file, err := parser.ParseFile(token.NewFileSet(), "hooks.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			descriptor := &x.Type{Name: "Rules", PkgPath: "example.com/hooks", SynteticType: &model.Type{Name: "Rules", PkgPath: "example.com/hooks", TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("Rules"), Type: &ast.StructType{Fields: &ast.FieldList{}}}, Imports: map[string]*model.ImportRef{"c": {Path: "context"}, "h": {Path: "github.com/viant/xdatly/handler"}, "e": {Path: "example.com/generated"}}}}
			for _, declaration := range file.Decls {
				descriptor.SynteticType.PtrMethodsAST = append(descriptor.SynteticType.PtrMethodsAST, declaration.(*ast.FuncDecl))
			}
			if err = catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "app", Package: "example.com/hooks"}}})
			if err != nil {
				t.Fatal(err)
			}
			childView := &spec.View{Name: "Lookup"}
			rootView := &spec.View{Name: "Orders", EntityHooks: "app.Rules", Relations: []*spec.Relation{{Holder: "Lookup", View: childView}}}
			rootID, _ := rootView.Identity()
			childID, _ := childView.Identity()
			child := &plan.RecordPlan{Identity: childID, InputPath: plan.FieldPath{"Input", "Orders", "Lookup"}, Auxiliary: true, Cardinality: spec.CardinalityOne}
			root := &plan.RecordPlan{Identity: rootID, InputPath: plan.FieldPath{"Input", "Orders"}, Cardinality: spec.CardinalityMany, Entity: &plan.EntityPlan{Owned: true}, Write: plan.WritePolicy{Allowed: []plan.Action{plan.ActionInsert, plan.ActionUpdate}}, Relations: []*plan.RelationPlan{{Child: child}}}
			if tc.mutate != nil {
				tc.mutate(root)
			}
			generated := &gen.Plan{Input: gen.ContractPlan{Type: "Input"}, Output: gen.ContractPlan{Type: "Output"}, Views: []gen.ViewPlan{{Identity: rootID, Type: "Order", Ownership: gen.ViewGenerated}, {Identity: childID, Type: "Lookup", Ownership: gen.ViewGenerated}}}
			generation := newHandlerGeneration(&Result{Component: &spec.Component{RootView: rootView}}, &gen.Input{TypeResolver: resolver, TargetPackage: "example.com/generated"}, Options{Handler: HandlerOptions{Go: GoHandlerOptions{Execution: GoExecutionMutation}}})
			err = generation.compileEntityHooks(&plan.Plan{Root: root}, generated, []handlergo.RecordType{{Identity: rootID, Path: root.InputPath, Value: "[]*Order"}, {Identity: childID, Path: child.InputPath, Value: "*Lookup"}})
			if (err != nil) != tc.invalid {
				t.Fatalf("compileEntityHooks=%v invalid=%v", err, tc.invalid)
			}
			if tc.invalid && !strings.Contains(err.Error(), "WriteEligible") {
				t.Fatalf("optional hook admission diagnostic=%v", err)
			}
		})
	}
}

func TestWriteEligibilityAdmissionRequiresCanonicalRoot(t *testing.T) {
	root := &plan.RecordPlan{Entity: &plan.EntityPlan{}, Write: plan.WritePolicy{Allowed: []plan.Action{plan.ActionInsert, plan.ActionUpdate}}}
	if !writeEligibilityAllowed(root, true) {
		t.Fatal("canonical root rejected")
	}
	if writeEligibilityAllowed(root, false) {
		t.Fatal("nested role admitted")
	}
	if writeEligibilityAllowed(nil, true) {
		t.Fatal("missing role admitted")
	}
	root.Entity.HooksComponent = true
	if writeEligibilityAllowed(root, true) {
		t.Fatal("component lifecycle admitted")
	}
}

// This exercises the second admission path: an unresolved lifecycle proposal
// whose create-once destination already has an authored optional method.
func TestHandlerGenerationWriteEligibilityScaffoldExistingSource(t *testing.T) {
	for _, tc := range []struct {
		name      string
		deleted   bool
		malformed bool
		invalid   bool
	}{
		{name: "valid authored optional method"},
		{name: "delete enabled authored optional method", deleted: true, invalid: true},
		{name: "malformed authored optional method", malformed: true, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := &plan.RecordPlan{Identity: "orders", InputPath: plan.FieldPath{"Input", "Orders"}, Table: "ORDERS", Cardinality: spec.CardinalityMany, Keys: []plan.KeyPart{{Field: "ID", Type: spec.TypeRef{Name: "int"}}}, Entity: &plan.EntityPlan{Hooks: spec.TypeRef{Package: "example.com/scaffold", Name: "Rules"}, HooksScaffold: true}, Write: plan.WritePolicy{Allowed: []plan.Action{plan.ActionUpdate}, Existing: plan.ActionUpdate}}
			root.Write.ValuePath = append(plan.FieldPath(nil), root.InputPath...)
			if tc.deleted {
				root.Write.Allowed = append(root.Write.Allowed, plan.ActionDelete)
				root.Write.DeleteMarker.Field = "Deleted"
			}
			semantic := &plan.Plan{Operation: plan.OperationPut, Root: root, Input: plan.ContractRef{Path: root.InputPath, Cardinality: spec.CardinalityMany}}
			config := handlergo.Config{Package: "scaffold", PackagePath: "example.com/scaffold", Factory: "NewWriter", InputType: "Input", OutputType: "Output", Records: []handlergo.RecordType{{Identity: root.Identity, Path: root.InputPath, Value: "[]*Order"}}}
			proposal, err := handlergo.ScaffoldMutationHooks(semantic, config)
			if err != nil {
				t.Fatal(err)
			}
			var source bytes.Buffer
			if err = format.Node(&source, token.NewFileSet(), proposal.File); err != nil {
				t.Fatal(err)
			}
			result := "(bool,error)"
			body := "return true,nil"
			if tc.malformed {
				result = "error"
				body = "return nil"
			}
			authored := source.String() + "\nfunc(*Rules)WriteEligible(context.Context,*Order,xhandler.LifecycleContext[Order,xhandler.NoParent,Output],xhandler.WriteAction)" + result + "{" + body + "}\n"
			directory := t.TempDir()
			destination := filepath.Join(directory, "lifecycle.go")
			if err = os.WriteFile(destination, []byte(authored), 0600); err != nil {
				t.Fatal(err)
			}
			generation := newHandlerGeneration(&Result{Source: &Source{Types: typecatalog.NewCatalog()}}, &gen.Input{}, Options{Handler: HandlerOptions{Hooks: HookOptions{Scaffold: true}, Go: GoHandlerOptions{Execution: GoExecutionMutation}}})
			generation.directory = directory
			_, err = generation.prepareMutationScaffold(semantic, config)
			if (err != nil) != tc.invalid {
				t.Fatalf("prepareMutationScaffold=%v invalid=%v", err, tc.invalid)
			}
			if tc.invalid && !strings.Contains(err.Error(), "WriteEligible") {
				t.Fatalf("optional hook diagnostic=%v", err)
			}
			actual, readErr := os.ReadFile(destination)
			if readErr != nil || string(actual) != authored {
				t.Fatalf("authored scaffold changed: %v", readErr)
			}
		})
	}
}
