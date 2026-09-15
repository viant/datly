package golang

import (
	"go/ast"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestGeneratedDeleteCallerTransactionSQLite(t *testing.T) {
	for _, operation := range []plan.Operation{plan.OperationPatch, plan.OperationPut} {
		t.Run(string(operation), func(t *testing.T) { runDeleteCallerTransaction(t, operation) })
	}
}

func runDeleteCallerTransaction(t *testing.T, operation plan.Operation) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	root := semantic.Root
	root.Table = "records"
	root.Sequence = nil
	if operation == plan.OperationPut {
		semantic.Operation = operation
		root.Write.Missing = ""
		root.Write.Allowed = []plan.Action{plan.ActionUpdate}
	}
	root.Write.DeleteMarker = plan.FieldRef{Field: "Remove", Type: spec.TypeRef{Name: "bool"}}
	root.Write.Allowed = append(root.Write.Allowed, plan.ActionDelete)
	root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Keys: root.Keys, Hooks: spec.TypeRef{Name: "Hooks"}, HooksBind: true, Fields: []plan.EntityField{
		{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true},
		{Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true},
		{Name: "Remove", Type: spec.TypeRef{Name: "bool"}, DeleteMarker: true},
	}}
	root.Current.Fields = []plan.CurrentField{{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect}, {Current: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Entity: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Conversion: plan.LinkDirect}}
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
		"type Marker struct{Id,Name bool}", "type Marker struct{Id,Name,Remove bool}",
		"type Record struct{", "type Record struct{Remove bool `sqlx:\"-\"`;",
		`[]string{"success","override","binding","validate","queue","mutate queued"}`, `[]string{"caller"}`,
		"  definition:=", "  events[0].Remove=true;events[0].Has.Remove=true;definition:=",
		"  result,err:=engine.New().Execute", "  suppliedTx,err:=h.DB.BeginTx(ctx,nil);if err!=nil{t.Fatal(err)};defer suppliedTx.Rollback();result,err:=engine.New().Execute",
		"DataSource:dml.Source{DB:h.DB}", "DataSource:dml.Source{DB:h.DB,Tx:suppliedTx}",
	).Replace(programSQLiteFixture)
	begin := strings.Index(source, `  success:=`)
	end := begin + strings.Index(source[begin:], ` })}`)
	source = source[:begin] + `  _=result
 if err!=nil{t.Fatal(err)}
 if len(outcomes)!=1||outcomes[0].CommitConfirmed()||outcomes[0].State()!=handler.TransactionCallerPending{t.Fatalf("transaction ownership changed: %+v",outcomes)}
 var n int;if err=suppliedTx.QueryRow("SELECT COUNT(*) FROM records WHERE id=1").Scan(&n);err!=nil||n!=0{t.Fatal("delete not visible inside caller transaction",err)}
 if err=suppliedTx.QueryRow("SELECT COUNT(*) FROM records WHERE id=2").Scan(&n);err!=nil||n!=1{t.Fatal("insert not visible inside caller transaction",err)}
 if err=suppliedTx.Rollback();err!=nil{t.Fatal("framework completed caller transaction",err)}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM records"},[]struct{Id int;Name string}{{1,"old"}})
 ` + source[end:]

	if operation == plan.OperationPut {
		source = strings.Replace(source, "INSERT INTO records VALUES(1,'old')", "INSERT INTO records VALUES(1,'old'),(2,'old')", 1)
		source = strings.Replace(source, "*state.Previous.Id!=1||", "(*state.Previous.Id!=1&&*state.Previous.Id!=2)||", 1)
		source = strings.Replace(source, `[]struct{Id int;Name string}{{1,"old"}}`, `[]struct{Id int;Name string}{{1,"old"},{2,"old"}}`, 1)
	}
	(entitySyncFixture{entity: asset.Entities, products: products, source: source}).run(t)
}
