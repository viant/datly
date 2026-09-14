package golang

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestMutationInsertIdentityReadinessSQLite(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPost, false)
	semantic.Root.Sequence = nil
	semantic.Root.Keys[0].Type = spec.TypeRef{Name: "int64"}
	semantic.Root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Keys: semantic.Root.Keys, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "int64"}, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}}}
	types := rootRecordTypes(semantic, "[]*Record", "")
	config := Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types}
	entities, err := EntitySupport(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := MutationFrameSupport(semantic, config, entities)
	if err != nil {
		t.Fatal(err)
	}
	actions, err := MutationActionSupport(semantic, config, entities, frames)
	if err != nil {
		t.Fatal(err)
	}
	runEntitySyncFixture(t, semantic, types, `package events
import("context";"strings";"testing";"github.com/viant/datly/internal/testharness/sqlite";"github.com/viant/datly/sql/dml";"github.com/viant/xdatly/handler";_ "github.com/viant/sqlx/metadata/product/sqlite")
type Marker struct{Id,Name bool}
`+"type Record struct{Id int64 `sqlx:\"id,primaryKey\"`;Name string `sqlx:\"name\"`;Has *Marker `sqlx:\"-\" setMarker:\"true\"`}\n"+`
type Input struct{Events []*Record};type Output struct{Data []*Record}
type binder struct{data *dml.Data;input *Input};func(b binder)Bind(ctx context.Context,target any)error{a:=target.(*_newEventsHandlerMutationActions);a.DML=b.data;a.Input=b.input;return nil};func(b binder)Lookup(context.Context,handler.ValueKey)(any,bool,error){return nil,false,nil}
func TestReadiness(t *testing.T){
 for _,tc:=range []struct{name string;original bool;mutate func(*Record);wantError string;wantID int64}{
  {"original explicit zero",true,nil,"",0},
  {"unresolved scalar zero",false,nil,"no completed producer",0},
  {"setter derived zero",false,func(r *Record){r.SetId(0)},"",0},
  {"derived nonzero",false,func(r *Record){r.SetId(9)},"",9},
 }{t.Run(tc.name,func(t *testing.T){
  ctx:=context.Background();h:=sqlite.New(t);if err:=h.ExecStatements(ctx,"CREATE TABLE EVENTS(id INTEGER PRIMARY KEY,name TEXT)");err!=nil{t.Fatal(err)}
  input:=&Input{Events:[]*Record{{Name:"row",Has:&Marker{Id:tc.original,Name:true}}}}
  original,err:=_newEventsHandlerCaptureInput(ctx,input);if err!=nil{t.Fatal(err)}
  database,err:=_newEventsHandlerDatabaseSnapshotCapture(input,nil);if err!=nil{t.Fatal(err)}
  if tc.mutate!=nil{tc.mutate(input.Events[0])}
  sync,err:=original.(*_newEventsHandlerOriginalInput).synchronize(input);if err!=nil{t.Fatal(err)};frames,err:=database.Build(input,sync);if err!=nil{t.Fatal(err)}
  native:=dml.NewData(h.DB);actions:=&_newEventsHandlerMutationActions{};if err=actions.Prepare(ctx,binder{native,input});err!=nil{t.Fatal(err)}
  if err=actions.Sequence(ctx,frames);err!=nil{t.Fatal(err)};err=actions.Diff(ctx,frames)
  if tc.wantError!=""{if err==nil||!strings.Contains(err.Error(),tc.wantError){t.Fatalf("error %v",err)};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS n FROM EVENTS"},[]struct{N int}{{0}});return}
  if err!=nil{t.Fatal(err)};if err=actions.Reconcile(ctx,frames);err!=nil{t.Fatal(err)};if err=actions.Queue(ctx,frames);err!=nil{t.Fatal(err)};if err=actions.VerifyQueued(ctx,frames);err!=nil{t.Fatal(err)}
  if err=native.Flush(ctx,"");err!=nil{t.Fatal(err)}
  if input.Events[0].Id!=tc.wantID||frames.Role0[0].State.Original.Has("Id")!=tc.original{t.Fatal("working/original identity mismatch")}
  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM EVENTS"},[]struct{Id int64;Name string}{{tc.wantID,"row"}})
 })}
}
`, frames.File, frames.Previous.File, frames.Layout.File, actions.File)
}
