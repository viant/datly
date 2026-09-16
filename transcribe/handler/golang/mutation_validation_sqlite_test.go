package golang

import (
	"go/ast"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestGenericPutRequiresActualPrevious(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPut, false)
	semantic.Root.Current = nil
	semantic.Root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Keys: semantic.Root.Keys, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}}}
	types := rootRecordTypes(semantic, "[]*Record", "")
	_, err := MutationProgram(semantic, Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types})
	if err == nil || !strings.Contains(err.Error(), "requires an authored Current prior read") {
		t.Fatalf("got %v", err)
	}
}

func TestGeneratedFrameworkValidationBeforeAllCustomHooksSQLite(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	semantic.Root.Table = "records"
	semantic.Root.Sequence = nil
	semantic.Root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Keys: semantic.Root.Keys, Hooks: spec.TypeRef{Name: "Hooks"}, HooksBind: true, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}}}
	semantic.Root.Current.Fields = []plan.CurrentField{{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect}, {Current: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Entity: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Conversion: plan.LinkDirect}}
	types := rootRecordTypes(semantic, "[]*Record", "[]*Previous")
	asset, err := MutationProgram(semantic, Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types})
	if err != nil {
		t.Fatal(err)
	}
	files, err := asset.Files()
	if err != nil {
		t.Fatal(err)
	}
	var products []*ast.File
	for _, file := range files {
		if file != asset.Entities.File {
			products = append(products, file)
		}
	}
	source := strings.NewReplacer(
		"{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition,
		"Name string `sqlx:\"name\"`", "Name string `sqlx:\"name\" validate:\"required\"`",
		"var outcomes []handler.Outcome", "var outcomes []handler.Outcome\nvar validationCalls int",
		"func(h *Hooks)Validate(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{", "func(h *Hooks)Validate(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{validationCalls++;",
		"[]string{\"success\",\"override\",\"binding\",\"validate\",\"queue\",\"mutate queued\"}", "[]string{\"success\",\"framework\"}",
		"outcomes=nil;finalizerKinds=nil;ctx,cancel:=", "outcomes=nil;finalizerKinds=nil;validationCalls=0;ctx,cancel:=",
		"  var supplied any=events", "  if mode==\"framework\"{events[0].Name=\"\";events[1].Name=\"\"};var supplied any=events",
		"success:=mode==\"success\"||mode==\"override\"", "if mode==\"framework\"{var failed *handler.Validation;if !errors.As(err,&failed)||len(failed.Violations)!=2||validationCalls!=0{t.Fatalf(\"framework error=%v custom calls=%d\",err,validationCalls)}};success:=mode==\"success\"||mode==\"override\"",
	).Replace(programSQLiteFixture)
	// Placeholder replacements in replacement text are intentionally a second
	// expansion; no generated policy source is parsed or rewritten here.
	source = strings.NewReplacer("{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition).Replace(source)
	(entitySyncFixture{entity: asset.Entities, products: products, source: source}).run(t)
}
