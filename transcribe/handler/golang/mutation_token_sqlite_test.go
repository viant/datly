package golang

import (
	"go/ast"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestGeneratedTimeTokenSQLite(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	root := semantic.Root
	root.Table = "records"
	root.Sequence = nil
	root.Write.ConcurrencyToken = plan.FieldRef{Field: "Version", Type: spec.TypeRef{Name: "clock.Time"}}
	root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Keys: root.Keys, Hooks: spec.TypeRef{Name: "Hooks"}, HooksBind: true, Fields: []plan.EntityField{
		{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true},
		{Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true},
		{Name: "Version", Type: spec.TypeRef{Name: "clock.Time"}, Writable: true, ConcurrencyToken: true},
	}}
	root.Current.Fields = nil
	for _, field := range root.Entity.Fields {
		root.Current.Fields = append(root.Current.Fields, plan.CurrentField{Current: plan.FieldRef{Field: field.Name, Type: field.Type}, Entity: plan.FieldRef{Field: field.Name, Type: field.Type}, Conversion: plan.LinkDirect})
	}
	types := rootRecordTypes(semantic, "[]*Record", "[]*Previous")
	asset, err := MutationProgram(semantic, Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types, Imports: []spec.ImportSpec{{Alias: "clock", Package: "time"}}})
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
		"type Marker struct{Id,Name bool}", "type Marker struct{Id,Name,Version bool}",
		"Name string `sqlx:\"name\"`", "Name string `sqlx:\"name\"`;Version time.Time `sqlx:\"version\"`",
		"name TEXT)", "name TEXT,version DATETIME)",
		"VALUES(1,'old')", "VALUES(1,'old','2026-09-01T00:00:00Z')",
		"SELECT id,name FROM records\"}}", "SELECT id,name,version FROM records\"}}",
		`[]string{"success","override","binding","validate","queue","mutate queued"}`, `[]string{"success","mismatch","missing","prepare next","missing previous","mutate validation"}`,
		"var outcomes []handler.Outcome", "var outcomes []handler.Outcome\nvar customCalls int",
		"outcomes=nil;finalizerKinds=nil;ctx,cancel:=", "outcomes=nil;finalizerKinds=nil;customCalls=0;ctx,cancel:=",
		"  dependencies,err:=", "  if mode==\"missing previous\"{component.Views[0].Source.SQL=\"SELECT id,name FROM records\"};dependencies,err:=",
		"  definition:=", `  tokenInstant:=time.Date(2026,9,1,2,0,0,0,time.FixedZone("same instant",7200));for _,row:=range events{row.Version=tokenInstant;row.Has.Version=true};if mode=="mismatch"{events[0].Version=tokenInstant.Add(time.Second)};if mode=="missing"{events[0].Has.Version=false};definition:=`,
		"h.initialized++;return nil", `if h.Input.Mode=="prepare next"{row.SetVersion(row.Version.Add(time.Hour))};h.initialized++;return nil`,
		"func(h *Hooks)Validate(_ context.Context,_ *Record,state handler.LifecycleContext[Record,handler.NoParent,Output])error{", "func(h *Hooks)Validate(_ context.Context,row *Record,state handler.LifecycleContext[Record,handler.NoParent,Output])error{customCalls++;if h.Input.Mode==\"mutate validation\"{row.Version=row.Version.Add(time.Hour)};",
		`success:=mode=="success"||mode=="override"`, `success:=mode=="success"||mode=="prepare next";if mode=="mismatch"||mode=="missing"||mode=="missing previous"{var conflict *handler.Conflict;if !errors.As(err,&conflict)||customCalls!=0{t.Fatalf("typed conflict/order: %v calls=%d",err,customCalls)}}`,
	).Replace(programSQLiteFixture)
	source = strings.NewReplacer("{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition).Replace(source)
	(entitySyncFixture{entity: asset.Entities, products: products, source: source}).run(t)
}
