package golang

import (
	"bytes"
	"encoding/json"
	"go/format"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	xshape "github.com/viant/x/shape"
)

func TestGeneratedInvariantBackfill(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPost, false)
	semantic.Root.Entity = &plan.EntityPlan{Owned: true, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Name: "RecordHas"}, Fields: []plan.EntityField{
		{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true},
		{Name: "Start", Type: spec.TypeRef{Name: "int"}, Writable: true},
		{Name: "End", Type: spec.TypeRef{Name: "*int"}, Writable: true},
		{Name: "Zone", Type: spec.TypeRef{Name: "string"}, Writable: true},
		{Name: "Days", Type: spec.TypeRef{Name: "[]string"}, Writable: true},
		{Name: "Enabled", Type: spec.TypeRef{Name: "bool"}, Writable: true},
		{Name: "Limit", Type: spec.TypeRef{Name: "int"}, Writable: true},
		{Name: "Burst", Type: spec.TypeRef{Name: "int"}, Writable: true},
		{Name: "Detail", Type: spec.TypeRef{Name: "any"}, Writable: true},
	}, Invariants: []plan.InvariantGroup{
		{Name: "Schedule", Fields: []string{"Start", "End", "Zone", "Days", "Enabled"}},
		{Name: "RatePolicy", Fields: []string{"Limit", "Burst", "Detail"}},
	}}
	before, err := json.Marshal(semantic)
	if err != nil {
		t.Fatal(err)
	}
	asset, err := EntitySupport(semantic, Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: rootRecordTypes(semantic, "[]*Record", "")})
	if err != nil {
		t.Fatal(err)
	}
	var source bytes.Buffer
	if err = format.Node(&source, token.NewFileSet(), asset.File); err != nil {
		t.Fatal(err)
	}
	after, err := json.Marshal(semantic)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("entity lowering mutated the semantic plan")
	}
	projected, err := (xshape.SourceParser{}).ProjectMethods(source.Bytes(), xshape.MethodProjection{Receivers: []string{"Record"}, Package: "models"})
	if err != nil {
		t.Fatalf("receiver behavior depends on component support: %v", err)
	}
	if strings.Contains(string(projected.Selected), asset.SnapshotType) {
		t.Fatal("receiver acquired component-private snapshot ownership")
	}
	root := t.TempDir()
	(testharness.GeneratedModule{}).Write(t, root)
	for name, content := range map[string][]byte{"entities.go": source.Bytes(), "entities_test.go": []byte(invariantBackfillContract)} {
		if err = os.WriteFile(filepath.Join(root, name), content, 0644); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.Command("go", "test", "-mod=mod", "-race", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated invariant helpers: %v\n%s\n%s", err, output, source.Bytes())
	}
}

const invariantBackfillContract = `package events
import("testing";"reflect")
type RecordHas struct{Id,Start,End,Zone,Days,Enabled,Limit,Burst,Detail bool}
type Record struct{Id *int64;Start int;End *int;Zone string;Days []string;Enabled bool;Limit,Burst int;Detail any;Has *RecordHas}
type Input struct{Events []*Record}
type Output struct{Data []*Record}
type fields map[string]bool
func(f fields)Has(name string)bool{return f[name]}
func TestBackfillFailureAtomicity(t *testing.T){
 for _,test:=range []struct{name string;detail any;marked bool;invalid bool}{
  {name:"omitted unsupported channel",detail:make(chan int),invalid:true},
  {name:"omitted unsupported function",detail:func(){},invalid:true},
  {name:"explicit null skips unsupported previous",detail:make(chan int),marked:true},
  {name:"clone mutable detail",detail:map[string][]int{"values":{1,2}}},
 }{t.Run(test.name,func(t *testing.T){
  previous:=&Record{Limit:10,Burst:20,Detail:test.detail}
  current:=&Record{Burst:7,Has:&RecordHas{Limit:true,Detail:test.marked}}
  marker:=*current.Has
  err:=current.BackfillRatePolicyIfNeeded(previous,nil)
  if (err!=nil)!=test.invalid{t.Fatalf("error=%v",err)}
  if current.Limit!=0||*current.Has!=marker{t.Fatal("explicit values or markers changed")}
  if test.invalid{
   if current.Burst!=7||current.Detail!=nil{t.Fatal("failed clone partially applied group")}
   return
  }
  if current.Burst!=20{t.Fatal("successful backfill omitted sibling")}
  if test.marked{if current.Detail!=nil{t.Fatal("explicit null was replaced")};return}
  detail:=current.Detail.(map[string][]int);detail["values"][0]=99
  if previous.Detail.(map[string][]int)["values"][0]!=1{t.Fatal("backfill aliases previous detail")}
 })}
}
func TestIndependentInvariantGroups(t *testing.T){
 for _,test:=range []struct{name string;schedule,rate bool}{
  {name:"neither"},{name:"schedule only",schedule:true},{name:"rate only",rate:true},{name:"both",schedule:true,rate:true},
 }{t.Run(test.name,func(t *testing.T){
  previous:=&Record{Start:3,Zone:"UTC",Limit:10,Burst:20}
  current:=&Record{}
  if test.schedule{current.SetStart(0)}
  if test.rate{current.SetLimit(0)}
  if current.HasScheduleChanges()!=test.schedule||current.HasRatePolicyChanges()!=test.rate{t.Fatal("group activation leaked")}
  originalMarker:=RecordHas{};if current.Has!=nil{originalMarker=*current.Has}
  for iteration:=0;iteration<2;iteration++{
   if err:=current.BackfillScheduleIfNeeded(previous,nil);err!=nil{t.Fatal(err)}
   if err:=current.BackfillRatePolicyIfNeeded(previous,nil);err!=nil{t.Fatal(err)}
  }
  if current.Start!=0||current.Limit!=0{t.Fatal("backfill replaced explicit zero")}
  wantZone:="";if test.schedule{wantZone="UTC"}
  wantBurst:=0;if test.rate{wantBurst=20}
  if current.Zone!=wantZone||current.Burst!=wantBurst{t.Fatalf("unexpected group hydration: %#v",current)}
  if current.Has!=nil&&*current.Has!=originalMarker{t.Fatal("backfill changed markers")}
  if !test.schedule&&!test.rate&&current.Has!=nil{t.Fatal("inactive groups allocated marker")}
 })}
}
func TestBackfill(t *testing.T){
 end:=9;previous:=&Record{Start:4,End:&end,Zone:"UTC",Days:[]string{"Mon"},Enabled:true}
 complete:=fields{"Start":true,"End":true,"Zone":true,"Days":true,"Enabled":true}
 for _,test:=range []struct{name string;current *Record;previous *Record;loaded fields;invalid bool;want Record}{
  {name:"nil marker",current:&Record{},previous:previous,want:Record{}},
  {name:"unrelated marker",current:&Record{Has:&RecordHas{Id:true}},previous:previous,want:Record{Has:&RecordHas{Id:true}}},
  {name:"new with omitted values",current:&Record{Start:1,Has:&RecordHas{Start:true}},want:Record{Start:1,Has:&RecordHas{Start:true}}},
  {name:"explicit zero null false",current:&Record{Has:&RecordHas{Start:true,End:true,Enabled:true}},previous:previous,loaded:complete,want:Record{Zone:"UTC",Days:[]string{"Mon"},Has:&RecordHas{Start:true,End:true,Enabled:true}}},
  {name:"missing projected member",current:&Record{Start:1,Has:&RecordHas{Start:true}},previous:previous,loaded:fields{"End":true},invalid:true,want:Record{Start:1,Has:&RecordHas{Start:true}}},
  {name:"complete without previous",current:&Record{Has:&RecordHas{Start:true,End:true,Zone:true,Days:true,Enabled:true}},want:Record{Has:&RecordHas{Start:true,End:true,Zone:true,Days:true,Enabled:true}}},
 } {
  t.Run(test.name,func(t *testing.T){
   var err error
   if test.loaded==nil{err=test.current.BackfillScheduleIfNeeded(test.previous,nil)}else{err=test.current.BackfillScheduleIfNeeded(test.previous,test.loaded)}
   if (err!=nil)!=test.invalid{t.Fatalf("error = %v",err)}
   if !reflect.DeepEqual(*test.current,test.want){t.Fatalf("value = %#v, want %#v",*test.current,test.want)}
  })
 }
 current:=&Record{Has:&RecordHas{Start:true}}
 originalMarker:=*current.Has
 if err:=current.BackfillScheduleIfNeeded(previous,complete);err!=nil{t.Fatal(err)}
 if *current.Has!=originalMarker{t.Fatal("backfill marked hydrated omissions")}
 current.Days[0]="Tue";*current.End=12
 if previous.Days[0]!="Mon"||*previous.End!=9{t.Fatal("backfill retained mutable previous aliases")}
 current.SetZone("");if !current.Has.Zone||!current.HasScheduleChanges(){t.Fatal("setter presence missing")}
 var absent *Record;if absent.HasScheduleChanges(){t.Fatal("nil entity reported change")};if err:=absent.BackfillScheduleIfNeeded(previous,complete);err!=nil{t.Fatal(err)}
}
`
