package golang

import (
	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"testing"
)

func TestMutationFrameCaptureCarriersAndNilGuards(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	semantic.Root.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, Keys: semantic.Root.Keys, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}}}
	semantic.Root.Current.Fields = []plan.CurrentField{{Current: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Entity: plan.FieldRef{Field: "Id", Type: spec.TypeRef{Name: "*int64"}}, Conversion: plan.LinkDirect}}
	types := rootRecordTypes(semantic, "[]*Record", "[]*Previous")
	types[0].CurrentValue = "*Envelope"
	config := Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types}
	entities, err := EntitySupport(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := MutationFrameSupport(semantic, config, entities)
	if err != nil {
		t.Fatal(err)
	}
	runEntitySyncFixture(t, semantic, types, `package events
import("context";"fmt";"testing";"github.com/viant/xdatly/handler")
type Marker struct{Id bool};type Record struct{Id *int64;Has *Marker};type Previous struct{Id *int64}
type NamedRows []Previous
type Envelope struct{Payload struct{Records NamedRows};Count int;One Previous}
type Input struct{Events []*Record;CurrentEvents *Envelope};type Output struct{Data []*Record}
type fieldSet struct{};func(fieldSet)Has(name string)bool{return name=="Id"}
type metadata struct{projection handler.ReadProjection};func(m *metadata)Projection(name string)(handler.ReadProjection,error){if name!="CurrentEvents"{return nil,fmt.Errorf("bad input path")};return m.projection,nil}
type projection struct{direct bool;holder string;fields handler.FieldSet}
func(p *projection)RootHolder()string{return p.holder};func(p *projection)DirectOutput()bool{return p.direct}
func(p *projection)Fields(root int,steps ...handler.ReadStep)(handler.FieldSet,error){return p.fields,nil}
func TestCarriers(t *testing.T){
 id:=int64(0);input:=&Input{Events:[]*Record{{Id:&id,Has:&Marker{Id:true}}},CurrentEvents:&Envelope{}}
 input.CurrentEvents.Payload.Records=NamedRows{{Id:&id}}
 valid:=&metadata{&projection{holder:"Payload.Records",fields:fieldSet{}}}
 for _,tc:=range []struct{name string;input *Input;metadata handler.ReadMetadata;fail bool}{
  {"wrapped named value rows",input,valid,false},
  {"nil wrapper empty read",&Input{},valid,false},
  {"nil input",nil,valid,true},
  {"nil metadata",input,nil,true},
  {"typed nil metadata",input,(*metadata)(nil),true},
  {"nil projection",input,&metadata{},true},
  {"typed nil projection",input,&metadata{(*projection)(nil)},true},
  {"missing holder",input,&metadata{&projection{}},true},
  {"unknown holder",input,&metadata{&projection{holder:"Missing"}},true},
  {"wrong holder type on nil wrapper",&Input{},&metadata{&projection{holder:"Count"}},true},
  {"nil wrapper singleton is empty",&Input{},&metadata{&projection{holder:"One",fields:fieldSet{}}},false},
  {"conflicting direct holder",input,&metadata{&projection{direct:true,holder:"Payload.Records"}},true},
  {"wrong direct shape",input,&metadata{&projection{direct:true}},true},
  {"nil fields",input,&metadata{&projection{holder:"Payload.Records"}},true},
  {"typed nil fields",input,&metadata{&projection{holder:"Payload.Records",fields:(*fieldSet)(nil)}},true},
 }{t.Run(tc.name,func(t *testing.T){got,err:=_newEventsHandlerDatabaseSnapshotCapture(tc.input,tc.metadata);if (err!=nil)!=tc.fail{t.Fatalf("got%v err%v",got,err)};if err!=nil&&got!=nil{t.Fatal("partial capture returned")}})}
 original,err:=_newEventsHandlerCaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)}
 synced,err:=original.(*_newEventsHandlerOriginalInput).synchronize(input);if err!=nil{t.Fatal(err)}
 db,err:=_newEventsHandlerDatabaseSnapshotCapture(input,valid);if err!=nil{t.Fatal(err)}
 for _,tc:=range []struct{name string;database *_newEventsHandlerDatabaseSnapshot;input *Input;sync *_newEventsHandlerPresenceSync}{
  {"nil database",nil,input,synced},{"nil input",db,nil,synced},{"nil sync",db,input,nil},
 }{t.Run(tc.name,func(t *testing.T){if got,err:=tc.database.Build(tc.input,tc.sync);err==nil||got!=nil{t.Fatalf("got%v err%v",got,err)}})}
}
`, frames.File, frames.Previous.File, frames.Layout.File)
}
