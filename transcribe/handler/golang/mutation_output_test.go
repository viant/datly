package golang

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestMutationOutputProjectsWorkingBody(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPost, false)
	semantic.Root.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, Keys: semantic.Root.Keys, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}}}
	semantic.Output.Path = plan.FieldPath{"Output", "Envelope", "Rows"}
	types := rootRecordTypes(semantic, "[]*Record", "")
	asset, err := MutationOutputSupport(semantic, Config{Package: "events", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types})
	if err != nil {
		t.Fatal(err)
	}
	runEntitySyncFixture(t, semantic, types, strings.ReplaceAll(mutationOutputFixture, "{{PROJECT}}", asset.Function), asset.File)
}

const mutationOutputFixture = `package events
import "testing"
type Marker struct{Id bool}
type Record struct{Id *int64;Has *Marker}
type Input struct{Events []*Record;Previous []*Record}
type Nested struct{Rows []*Record}
type Output struct{Envelope *Nested;Status string}
func TestOutput(t *testing.T){
 id,old:=int64(7),int64(99)
 current:=&Record{Id:&id};input:=&Input{Events:[]*Record{current},Previous:[]*Record{{Id:&old}}}
 output:=&Output{Status:"retained"}
 if err:={{PROJECT}}(input,output);err!=nil{t.Fatal(err)}
 if output.Envelope==nil||len(output.Envelope.Rows)!=1||output.Envelope.Rows[0]!=current||*output.Envelope.Rows[0].Id!=7||output.Status!="retained"{t.Fatalf("output=%+v",output)}
 holder:=output.Envelope;id=8;input.Events=append(input.Events,nil)
 if err:={{PROJECT}}(input,output);err!=nil{t.Fatal(err)}
 if output.Envelope!=holder||len(holder.Rows)!=2||*holder.Rows[0].Id!=8||holder.Rows[1]!=nil{t.Fatal("transformed body shape/order lost")}
 if err:={{PROJECT}}(nil,output);err==nil{t.Fatal("nil input accepted")}
 if err:={{PROJECT}}(input,nil);err==nil{t.Fatal("nil output accepted")}
 input.Events=nil;if err:={{PROJECT}}(input,output);err!=nil{t.Fatal(err)};if holder.Rows!=nil{t.Fatal("nil body changed to empty collection")}
 input.Events=[]*Record{};if err:={{PROJECT}}(input,output);err!=nil{t.Fatal(err)};if holder.Rows==nil{t.Fatal("empty body changed to nil")}
}
`
