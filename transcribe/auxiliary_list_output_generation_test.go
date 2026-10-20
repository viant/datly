package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalAuxiliaryLifecycleIndependentListOutput(t *testing.T) {
	t.Run("list", func(t *testing.T) { testCanonicalAuxiliaryResult(t, false) })
	t.Run("compatible body", func(t *testing.T) { testCanonicalAuxiliaryResult(t, true) })
}
func testCanonicalAuxiliaryResult(t *testing.T, compatible bool) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE events(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	source := &Source{Name: "Events", Scope: "example.com/auxiliary", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#package('generated')
#setting($_ = $route('/events','PATCH'))
#define($_ = $Body<*EventsView>(body/).Optional())
#define($_ = $Data<[]int>(output/body).WithTag('json:"data"'))
SELECT e.*,lifecycle_type(e,'BatchLifecycle') FROM (events) e`}
	resultType := "[]int"
	if compatible {
		source.Text = strings.Replace(source.Text, "$Data<[]int>", "$Data<*EventsView>", 1)
		resultType = "*EventsView"
	}
	request := GenerationRequest{Source: source, Destination: root}
	generated, err := (Generator{Operation: "patch"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(generated.Result.Plan.Output.Fields) != 1 || generated.Result.Plan.Output.Fields[0].Name != "Data" || generated.Result.Plan.Output.Fields[0].Type != resultType {
		t.Fatalf("original list output changed: %+v", generated.Result.Plan.Output)
	}
	for _, f := range generated.Result.Files {
		if strings.Contains(f.Content, "output.Data = input.Body") || strings.Contains(f.Content, "CurEvents") {
			t.Fatalf("auxiliary lifecycle acquired entity assignment/current: %s", f.Path)
		}
	}
	// lifecycle.go is the authored, create-once hook scaffold, not a regenerated artifact.
	hook := `package generated
import("context";"fmt";"reflect";handler "github.com/viant/xdatly/handler")
var _anchorBatchLifecycle=reflect.TypeFor[BatchLifecycle]()
var canonicalInput *EventsInput
var canonicalOutput *EventsOutput
var initCalls,validateCalls,finalizeCalls int
type BatchLifecycle struct{}
func(*BatchLifecycle)Init(_ context.Context,input *EventsInput,state handler.LifecycleContext[EventsInput,handler.NoParent,EventsOutput])error{initCalls++;if input!=canonicalInput||state.Output==nil{return fmt.Errorf("wrong canonical input/output")};canonicalOutput=state.Output;state.Output.Data=[]int{};if input.Body!=nil&&input.Body.Id!=nil{state.Output.Data=append(state.Output.Data,*input.Body.Id)};return nil}
func(*BatchLifecycle)Validate(_ context.Context,input *EventsInput,state handler.LifecycleContext[EventsInput,handler.NoParent,EventsOutput])error{validateCalls++;if input!=canonicalInput||state.Output!=canonicalOutput{return fmt.Errorf("different validation contracts")};return nil}
func(*BatchLifecycle)Finalize(_ context.Context,input *EventsInput,output *EventsOutput,_ handler.Outcome)error{finalizeCalls++;if input!=canonicalInput||output!=canonicalOutput{return fmt.Errorf("different final contracts")};return nil}
`
	if compatible {
		hook = strings.Replace(hook, "state.Output.Data=[]int{};if input.Body!=nil&&input.Body.Id!=nil{state.Output.Data=append(state.Output.Data,*input.Body.Id)}", "if state.Output.Data!=input.Body{return fmt.Errorf(\"compatible output was not populated before Init\")}", 1)
	}
	if err = os.WriteFile(filepath.Join(root, "generated/lifecycle.go"), []byte(hook), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	runtime := `package generated
import("context";"encoding/json";"fmt";"reflect";"testing";"github.com/viant/datly/bootstrap";rhandler "github.com/viant/datly/runtime/handler";"github.com/viant/datly/runtime/handler/writer";dtag "github.com/viant/datly/tag";handler "github.com/viant/xdatly/handler")
type resultBinder struct{}
func(resultBinder)Bind(context.Context,any)error{return nil}
func(resultBinder)Lookup(context.Context,handler.ValueKey)(any,bool,error){return nil,false,fmt.Errorf("unexpected database capability")}
func TestGeneratedAuxiliaryListResult(t *testing.T){
 holder:=reflect.TypeFor[EventsComponent]();field,_:=holder.FieldByName("Contract");tag,ok,err:=dtag.ParseComponent(field.Tag);if err!=nil||!ok{t.Fatal(err)};if tag.Handler!=""{t.Fatal("custom factory retained")}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"EventsInput",OutputType:"EventsOutput"}
 component,err:=source.Resolve(reflect.TypeFor[EventsInput](),reflect.TypeFor[EventsOutput]());if err!=nil{t.Fatal(err)}
 metadata,err:=writer.Compile(component,reflect.TypeFor[EventsInput](),reflect.TypeFor[EventsOutput](),"patch");if err!=nil{t.Fatal(err)};if !metadata.Root.Auxiliary||metadata.OutputField!=-1{t.Fatal("list result acquired entity publication")}
 native,err:=writer.New(component,reflect.TypeFor[EventsInput](),reflect.TypeFor[EventsOutput](),"patch");if err!=nil{t.Fatal(err)}
 for _,body:=range []*EventsView{nil,{Id:func()*int{v:=8;return &v}()}}{
  canonicalInput=&EventsInput{Body:body};canonicalOutput=nil;initCalls=0;validateCalls=0;finalizeCalls=0
  snapshot,err:=native.CaptureInput(context.Background(),canonicalInput);if err!=nil{t.Fatal(err)}
  invocation:=rhandler.Invocation{Input:canonicalInput,Snapshot:snapshot,Binder:resultBinder{}}
  value,err:=native.Execute(context.Background(),invocation);if err!=nil{t.Fatal(err)};output:=value.(*EventsOutput)
  if output!=canonicalOutput||initCalls!=1||validateCalls!=1{t.Fatal("canonical lifecycle lost")}
  if err=native.FinalizeOutcome(context.Background(),invocation,output,handler.Outcome{});err!=nil||finalizeCalls!=1{t.Fatal(err)}
  raw,err:=json.Marshal(output);if err!=nil{t.Fatal(err)};want:="{\"data\":[]}";if body!=nil{want="{\"data\":[8]}"};if string(raw)!=want{t.Fatalf("response=%s want=%s",raw,want)}
 }
}
`
	if compatible {
		runtime = strings.Replace(runtime, "metadata.OutputField!=-1", "metadata.OutputField<0", 1)
		runtime = strings.Replace(runtime, "if string(raw)!=want{t.Fatalf(\"response=%s want=%s\",raw,want)}", "_ = raw;_ = want;if output.Data!=body{t.Fatal(\"compatible output pointer changed\")}", 1)
	}
	if err = os.WriteFile(filepath.Join(root, "generated/list_runtime_test.go"), []byte(runtime), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("canonical auxiliary list result runtime: %v\n%s", err, output)
	}
}
