package golang

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

// Direct semantic fixture: compilation from DQL is tested at transcribe.
func TestGeneratedLookupProjection(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, true)
	root := semantic.Root
	pointer := spec.TypeRef{Name: "*int64"}
	root.Keys[1].Type = pointer
	root.Entity = &plan.EntityPlan{Owned: true, Type: spec.TypeRef{Name: "Row"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "Marks"}, Keys: root.Keys, Fields: []plan.EntityField{
		{Name: "Id", Type: pointer, Writable: true, Identity: true}, {Name: "TenantId", Type: pointer, Writable: true, Identity: true}, {Name: "KindId", Type: pointer, Writable: true}, {Name: "Kinds", Type: spec.TypeRef{Name: "[]*Kind"}, Relation: true},
	}}
	for _, name := range []string{"Id", "TenantId", "KindId"} {
		root.Current.Fields = append(root.Current.Fields, plan.CurrentField{Entity: plan.FieldRef{Field: name, Type: pointer}, Current: plan.FieldRef{Field: name, Type: pointer}})
	}
	root.Current.Keys = append([]plan.KeyPart(nil), root.Keys...)
	child := &plan.RecordPlan{Auxiliary: true, Identity: "kind", InputPath: plan.FieldPath{"Input", "Events", "Kinds"}, Cardinality: spec.CardinalityMany, Keys: []plan.KeyPart{{Field: "Id", Source: "ID", Type: pointer}}, Current: &plan.CurrentPlan{InputPath: plan.FieldPath{"Input", "CurrentKinds"}, Keys: []plan.KeyPart{{Field: "Id", Source: "ID", Type: pointer}}, Lookup: &plan.LookupProjection{Name: "ProjectKinds", Columns: []string{"KIND_ID"}}}}
	child.Write = plan.WritePolicy{Order: 1, ValuePath: child.InputPath}
	root.Relations = []*plan.RelationPlan{{Identity: "Kinds", FieldPath: plan.FieldPath{"Kinds"}, Cardinality: spec.CardinalityMany, Child: child, Links: []plan.KeyLink{{Parent: plan.KeyPart{Field: "KindId", Source: "KIND_ID", Type: pointer}, Child: child.Keys[0]}}}}
	config := Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: []RecordType{{Identity: root.Identity, Path: root.InputPath, Value: "[]*Row", Current: "[]*Previous"}, {Identity: child.Identity, Path: child.InputPath, Value: "[]*Kind", Current: "[]*Kind"}}}
	asset, err := EntitySupport(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	(entitySyncFixture{entity: asset, source: lookupProjectionRuntime}).run(t)
}

const lookupProjectionRuntime = `package events
import("reflect";"strings";"testing")
type Marks struct{Id,TenantId,KindId,Kinds bool}
type Row struct{Id,TenantId,KindId *int64;Kinds []*Kind;Has *Marks}
type Previous struct{Id,TenantId,KindId *int64}
type Kind struct{Id *int64}
type Input struct{Events []*Row;CurrentEvents []*Previous;CurrentKinds []*Kind}
type Output struct{Data []*Row}
func ptr(value int64)*int64{return &value}
func TestLookup(t *testing.T){
 previous:=[]*Previous{{Id:ptr(5),TenantId:ptr(1),KindId:ptr(7)},{Id:ptr(5),TenantId:ptr(2),KindId:ptr(8)},{Id:ptr(0),TenantId:ptr(0),KindId:ptr(0)}}
 for _,tc:=range []struct{name string;row *Row;want []int64}{
 {"first tenant",&Row{Id:ptr(5),TenantId:ptr(1),Has:&Marks{Id:true,TenantId:true}},[]int64{7}},
 {"second tenant",&Row{Id:ptr(5),TenantId:ptr(2),Has:&Marks{Id:true,TenantId:true}},[]int64{8}},
 {"supplied FK",&Row{KindId:ptr(9),Has:&Marks{KindId:true}},[]int64{9}},
 {"explicit null",&Row{Id:ptr(5),TenantId:ptr(1),Has:&Marks{Id:true,TenantId:true,KindId:true}},nil},
 {"missing identity",&Row{Id:ptr(5),Has:&Marks{Id:true}},nil},
 {"null identity",&Row{Id:ptr(5),Has:&Marks{Id:true,TenantId:true}},nil},
 {"unmatched",&Row{Id:ptr(5),TenantId:ptr(3),Has:&Marks{Id:true,TenantId:true}},nil},
 {"zero identity",&Row{Id:ptr(0),TenantId:ptr(0),Has:&Marks{Id:true,TenantId:true}},[]int64{0}},
 }{t.Run(tc.name,func(t *testing.T){
  before:=*tc.row.Has;result,err:=(&Input{}).ProjectKinds([]*Row{tc.row},previous);if err!=nil{t.Fatal(err)}
  var values []int64;for _,row:=range result{values=append(values,row.Value0)}
  if !reflect.DeepEqual(values,tc.want)||*tc.row.Has!=before{t.Fatalf("got %v want %v; presence=%+v",values,tc.want,tc.row.Has)}
  if *previous[0].KindId!=7||*previous[1].KindId!=8{t.Fatal("Current snapshot was changed")}
 })}
 row:=&Row{KindId:ptr(7),Has:&Marks{KindId:true}}
 values,err:=(&Input{}).ProjectKinds([]*Row{row,row},previous);if err!=nil||len(values)!=1{t.Fatal("deduplication",err)}
 duplicate:=append(append([]*Previous(nil),previous...),&Previous{Id:ptr(5),TenantId:ptr(1),KindId:ptr(99)})
 if _,err:=(&Input{}).ProjectKinds([]*Row{row},duplicate);err==nil||!strings.Contains(err.Error(),"duplicated"){t.Fatalf("ambiguous Current accepted: %v",err)}
}
`
