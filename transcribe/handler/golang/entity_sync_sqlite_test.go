package golang

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestGeneratedPresencePreservesSparseSQLiteUpdates(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPost, false)
	semantic.Root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}, {Name: "Enabled", Type: spec.TypeRef{Name: "bool"}, Writable: true}}}
	runEntitySyncFixture(t, semantic, rootRecordTypes(semantic, "[]*Record", ""), sparseSyncSQLiteFixture)
}

const sparseSyncSQLiteFixture = "package events\nimport(\"context\";\"testing\";\"github.com/viant/datly/internal/testharness/sqlite\";\"github.com/viant/datly/sql/dml\")\n" +
	"type Marker struct{Id,Name,Enabled bool}\n" +
	"type Record struct{Id *int64 `sqlx:\"id,primaryKey\"`;Name string `sqlx:\"name\"`;Enabled bool `sqlx:\"enabled\"`;Has *Marker `setMarker:\"true\" sqlx:\"-\"`}\n" +
	"type Input struct{Events []*Record}\ntype Output struct{Data []*Record}\n" +
	`func TestSparseSQL(t *testing.T){
 for _,test:=range []struct{name string;nameSupplied,enabledSupplied,mutateEnabled bool;wantName string;wantEnabled bool}{
  {"omitted sibling",false,false,true,"retained",true},
  {"explicit empty",true,false,true,"",true},
  {"explicit false",false,true,false,"retained",false},
 }{t.Run(test.name,func(t *testing.T){
  ctx:=context.Background();h:=sqlite.New(t)
  if err:=h.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT,enabled BOOLEAN)","INSERT INTO records(id,name,enabled)VALUES(0,'retained',1)");err!=nil{t.Fatal(err)}
  zero:=int64(0);record:=&Record{Id:&zero,Has:&Marker{Id:true,Name:test.nameSupplied,Enabled:test.enabledSupplied}};input:=&Input{Events:[]*Record{record}}
  captured,err:=_newEventsHandlerCaptureInput(ctx,input);if err!=nil{t.Fatal(err)};snapshot:=captured.(*_newEventsHandlerOriginalInput)
  record.Has.Name=false;record.Has.Enabled=false
  if test.mutateEnabled{record.Enabled=true}
  if err:=snapshot.SyncPresence(input);err!=nil{t.Fatal(err)}
  data:=dml.NewData(h.DB);if err:=data.Update("records",record);err!=nil{t.Fatal(err)};if err:=data.Flush(ctx,"");err!=nil{t.Fatal(err)}
  type row struct{ID int;Name string;Enabled bool}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name,enabled FROM records ORDER BY id"},[]row{{ID:0,Name:test.wantName,Enabled:test.wantEnabled}})
  if snapshot.Roots[0].Has("Name")!=test.nameSupplied||snapshot.Roots[0].Has("Enabled")!=test.enabledSupplied{t.Fatal("DML path changed original presence")}
 })}
}
`
