package golang

import (
	"go/ast"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestGeneratedInsertDeleteSameIdentitySQLite(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	root := semantic.Root
	root.Table = "records"
	root.Sequence = nil
	root.Write.ActionPolicy = "insert-delete"
	root.Write.Existing = plan.ActionInsert
	root.Write.Missing = plan.ActionInsert
	root.Write.Allowed = []plan.Action{plan.ActionInsert, plan.ActionDelete}
	root.Write.DeleteMarker = plan.FieldRef{Field: "Remove", Type: spec.TypeRef{Name: "bool"}}
	root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marker"}, Keys: root.Keys, Hooks: spec.TypeRef{Name: "Hooks"}, HooksBind: true, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}, {Name: "Remove", Type: spec.TypeRef{Name: "bool"}, DeleteMarker: true}}}
	root.Entity.Invariants = []plan.InvariantGroup{{Name: "Details", Fields: []string{"Id", "Name"}}}
	root.Current.Fields = []plan.CurrentField{{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect}, {Current: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Entity: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Conversion: plan.LinkDirect}}
	asset, err := MutationProgram(semantic, Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: rootRecordTypes(semantic, "[]*Record", "[]*Previous")})
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
	source := strings.NewReplacer("{{FACTORY}}", asset.Factory, "{{DEFINITION}}", asset.Definition,
		"type Marker struct{Id,Name bool}", "type Marker struct{Id,Name,Remove bool}",
		"type Record struct{", "type Record struct{Remove bool `sqlx:\"-\"`;",
		"one,two:=int64(1),int64(2)", "one,two:=int64(1),int64(1)",
		"  definition:=", "  events[0].Remove=true;events[0].Has.Remove=true;definition:=",
		"*output.Data[1].Id!=2", "*output.Data[1].Id!=1",
		`expected=[]row{{Id:1,Name:"updated"},{Id:2,Name:"inserted"}}`, `expected=[]row{{Id:1,Name:"inserted"}}`,
	).Replace(programSQLiteFixture)
	source = strings.Replace(source, `"context";"errors"`, `"context";"errors";"strings"`, 1)
	source = strings.Replace(source, `[]string{"success","override","binding","validate","queue","mutate queued"}`, `[]string{"success","override","binding","validate","queue","mutate queued","reversed","duplicate inserts","duplicate deletes","three rows","sparse insert","sparse delete"}`, 1)
	source = strings.Replace(source, `"INSERT INTO records VALUES(1,'old')"`, `"INSERT INTO records VALUES(1,'old')","CREATE TABLE audit(action TEXT)","CREATE TRIGGER item_deleted AFTER DELETE ON records BEGIN INSERT INTO audit VALUES('delete'); END","CREATE TRIGGER item_inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES('insert'); END"`, 1)
	source = strings.Replace(source, `definition:=`, `if mode=="reversed"{events[0],events[1]=events[1],events[0]};if mode=="duplicate inserts"{events[0].Remove=false};if mode=="duplicate deletes"{events[1].Remove=true;events[1].Has.Remove=true};if mode=="three rows"{third:=int64(1);events=append(events,&Record{Id:&third,Name:"third",Has:&Marker{Id:true,Name:true}})};definition:=`, 1)
	source = strings.Replace(source, `CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)`, `CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT CHECK(name <> ''))`, 1)
	source = strings.Replace(source, `definition:=`, `if mode=="sparse insert"{events[1].Name="";events[1].Has.Name=false};if mode=="sparse delete"{events[0].Name="";events[0].Has.Name=false};definition:=`, 1)
	source = strings.Replace(source, `!state.Original.Has("Name")`, `(!state.Original.Has("Name")&&h.Input.Mode!="sparse insert"&&h.Input.Mode!="sparse delete")`, 1)
	source = strings.Replace(source, ` h.initialized++;return nil`, ` if h.Input.Mode=="sparse insert"&&!row.Remove&&(row.Name!=""||row.Has.Name){return errors.New("insert inherited previous invariant")};if h.Input.Mode=="sparse delete"&&row.Remove&&(row.Name!="old"||row.Has.Name){return errors.New("delete invariant or presence changed")};h.initialized++;return nil`, 1)

	source = strings.Replace(source, `  var supplied any=events`, `  if strings.HasPrefix(mode,"duplicate")||mode=="three rows"{definition.Finalizer=&completion{}}
  var supplied any=events`, 1)
	source = strings.Replace(source, `  success:=`, `  if mode=="sparse insert"&&(events[1].Name!=""||err==nil||!strings.Contains(err.Error(),"CHECK constraint")){t.Fatalf("incomplete insert must reach its own constraint without Previous backfill: name=%q err=%v",events[1].Name,err)}
  success:=`, 1)
	source = strings.Replace(source, `  success:=`, `  if strings.HasPrefix(mode,"duplicate")||mode=="three rows"{if err==nil||!strings.Contains(err.Error(),"duplicate"){t.Fatalf("duplicate pair admitted: %v",err)};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM records"},[]struct{Id int64;Name string}{{1,"old"}});var n int;if e:=h.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&n);e!=nil||n!=0{t.Fatalf("duplicate mutation persisted audit=%d err=%v",n,e)};return}
  success:=`, 1)
	source = strings.Replace(source, `success:=mode=="success"||mode=="override"`, `success:=mode=="success"||mode=="override"||mode=="reversed"||mode=="sparse delete"`, 1)
	source = strings.Replace(source, `  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM records ORDER BY id"},expected)`, `  h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM records ORDER BY id"},expected)
  auditExpected:=0;if success{auditExpected=2};var auditCount int;if e:=h.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&auditCount);e!=nil||auditCount!=auditExpected{t.Fatalf("audit=%d want=%d err=%v",auditCount,auditExpected,e)}
  if success{h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT action FROM audit ORDER BY rowid"},[]struct{Action string}{{"delete"},{"insert"}})}`, 1)
	(entitySyncFixture{entity: asset.Entities, products: products, source: source}).run(t)
}
