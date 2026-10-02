package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	tcolumn "github.com/viant/datly/transcribe/column"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestLeafAuxiliaryRootLifecycle(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, "CREATE TABLE events(id INTEGER PRIMARY KEY,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	source := &Source{Name: "Events", Scope: "example.com/auxiliary", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#setting($_ = $route('/events','PATCH'))
#define($_ = $Events<[]*EventsView>(body/data))
#define($_ = $Data<[]*EventsView>(output/body))
#define($_ = $Status<string>(output/status))
SELECT e.*,lifecycle_type(e,'BatchLifecycle') FROM (events) e`}
	request := Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Go: GoHandlerOptions{Execution: GoExecutionMutation, Factory: "NewEventsHandler"}, Hooks: HookOptions{Scaffold: true}}}}
	_, err := NewCompiler().Transcribe(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(root, "generated", "lifecycle.go")); err != nil {
		t.Fatalf("native auxiliary lifecycle scaffold missing: %v", err)
	}
	if err = os.WriteFile(filepath.Join(root, "generated", "lifecycle.go"), []byte(auxiliaryLifecycleSource), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewCompiler().Transcribe(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "generated", "auxiliary_test.go"), []byte(auxiliaryLifecycleRuntimeSource), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "-timeout", "60s", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("auxiliary lifecycle: %v\n%s", err, output)
	}
}

const auxiliaryLifecycleSource = `package events
import("context";"fmt";handler "github.com/viant/xdatly/handler")
var initCalls,validateCalls,finalizeCalls int
var failInit bool
var expectedInput *EventsInput
var expectedOutput *EventsOutput
type BatchLifecycle struct{}
func(h *BatchLifecycle)Init(ctx context.Context,input *EventsInput,state handler.LifecycleContext[EventsInput,handler.NoParent,EventsOutput])error{
 initCalls++;if expectedOutput==nil{expectedOutput=state.Output};if input!=expectedInput||state.Output!=expectedOutput||state.Previous!=nil||state.Parent!=nil||state.Original!=nil||state.SelfParent!=nil||state.PreviousFields!=nil{return fmt.Errorf("wrong canonical lifecycle")}
 state.Output.Status="hook-output";state.Output.Data=nil
 if failInit{return fmt.Errorf("owned batch failure")};return nil
}
func(h *BatchLifecycle)Validate(ctx context.Context,input *EventsInput,state handler.LifecycleContext[EventsInput,handler.NoParent,EventsOutput])error{validateCalls++;if state.Output!=expectedOutput{return fmt.Errorf("different output")};return nil}
func(h *BatchLifecycle)Finalize(ctx context.Context,input *EventsInput,output *EventsOutput,outcome handler.Outcome)error{finalizeCalls++;return nil}
type IsolatedLifecycle struct{ calls int; input *EventsInput; output *EventsOutput }
func(h *IsolatedLifecycle)Init(ctx context.Context,input *EventsInput,state handler.LifecycleContext[EventsInput,handler.NoParent,EventsOutput])error{h.calls++;h.input=input;h.output=state.Output;state.Output.Status=fmt.Sprint(len(input.Events));return nil}
func(h *IsolatedLifecycle)Validate(ctx context.Context,input *EventsInput,state handler.LifecycleContext[EventsInput,handler.NoParent,EventsOutput])error{if h.calls!=1||h.input!=input||h.output!=state.Output{return fmt.Errorf("shared invocation state")};return nil}
func(h *IsolatedLifecycle)Finalize(ctx context.Context,input *EventsInput,output *EventsOutput,outcome handler.Outcome)error{if h.input!=input||h.output!=output{return fmt.Errorf("wrong completion state")};return nil}
`
const auxiliaryLifecycleRuntimeSource = `package events
import("context";"fmt";"reflect";"testing";"sync";rhandler "github.com/viant/datly/runtime/handler";"github.com/viant/datly/runtime/handler/writer";"github.com/viant/datly/spec";handler "github.com/viant/xdatly/handler")
var linkedBatchType=reflect.TypeOf(BatchLifecycle{})
var linkedIsolatedType=reflect.TypeOf(IsolatedLifecycle{})
type batchBinder struct{}
func(batchBinder)Bind(context.Context,any)error{return nil}
func(batchBinder)Lookup(context.Context,handler.ValueKey)(any,bool,error){return nil,false,fmt.Errorf("unexpected database capability lookup")}
func TestAuxiliaryCanonicalInputOnce(t *testing.T){
 component:=&spec.Component{Key:spec.Key{Scope:linkedBatchType.PkgPath()},Settings:&spec.Settings{},RootView:&spec.View{Name:"e",Source:&spec.ViewSource{Table:"events"}}}
 h,err:=writer.New(component,reflect.TypeOf(EventsInput{}),reflect.TypeOf(EventsOutput{}),"patch");if err!=nil{t.Fatal(err)}
 for _,rows:=range [][]*EventsView{nil,{}, {new(EventsView),new(EventsView)}}{
  initCalls,validateCalls,finalizeCalls=0,0,0;expectedInput=&EventsInput{Events:rows};expectedOutput=nil
  snapshot,err:=h.CaptureInput(context.Background(),expectedInput);if err!=nil{t.Fatal(err)}
  invocation:=rhandler.Invocation{Input:expectedInput,Snapshot:snapshot,Binder:batchBinder{}}
  output,err:=h.Execute(context.Background(),invocation);if err!=nil{t.Fatal(err)}
  if initCalls!=1||validateCalls!=1||output!=expectedOutput||expectedOutput.Status!="hook-output"||expectedOutput.Data!=nil{t.Fatal("batch lifecycle or output overwritten")}
  if _,err=h.Execute(context.Background(),invocation);err==nil{t.Fatal("duplicate lifecycle accepted")}
  if err=h.FinalizeOutcome(context.Background(),invocation,output,handler.Outcome{});err!=nil||finalizeCalls!=1{t.Fatal("completion")}
  if err=h.FinalizeOutcome(context.Background(),invocation,output,handler.Outcome{});err!=nil||finalizeCalls!=1{t.Fatal("duplicate completion")}
 }
 failInit=true;defer func(){failInit=false}();expectedInput=&EventsInput{};expectedOutput=nil;validateCalls=0
 snapshot,err:=h.CaptureInput(context.Background(),expectedInput);if err!=nil{t.Fatal(err)}
 invocation:=rhandler.Invocation{Input:expectedInput,Snapshot:snapshot,Binder:batchBinder{}}
 output,err:=h.Execute(context.Background(),invocation);if err==nil||output!=expectedOutput||expectedOutput.Status!="hook-output"||validateCalls!=0{t.Fatal("failed Init continued or lost output")}
}
func TestAuxiliaryInvocationIsolation(t *testing.T){
 component:=&spec.Component{Key:spec.Key{Scope:linkedIsolatedType.PkgPath()},Settings:&spec.Settings{},RootView:&spec.View{Name:"e",Auxiliary:true,EntityHooks:"IsolatedLifecycle",Source:&spec.ViewSource{Table:"events"}}}
 h,err:=writer.New(component,reflect.TypeOf(EventsInput{}),reflect.TypeOf(EventsOutput{}),"patch");if err!=nil{t.Fatal(err)}
 var wait sync.WaitGroup
 for n:=0;n<16;n++{wait.Add(1);go func(n int){defer wait.Done();input:=&EventsInput{Events:make([]*EventsView,n)};for i:=range input.Events{input.Events[i]=new(EventsView)};snapshot,err:=h.CaptureInput(context.Background(),input);if err!=nil{t.Error(err);return};invocation:=rhandler.Invocation{Input:input,Snapshot:snapshot,Binder:batchBinder{}};result,err:=h.Execute(context.Background(),invocation);if err!=nil{t.Error(err);return};output:=result.(*EventsOutput);if output.Status!=fmt.Sprint(n){t.Error("cross-request output")};if err=h.FinalizeOutcome(context.Background(),invocation,output,handler.Outcome{});err!=nil{t.Error(err)}}(n)}
 wait.Wait()
}
func TestAuxiliaryCancellationPrecedesHook(t *testing.T){
 component:=&spec.Component{Key:spec.Key{Scope:linkedBatchType.PkgPath()},Settings:&spec.Settings{},RootView:&spec.View{Name:"e",Auxiliary:true,EntityHooks:"BatchLifecycle",Source:&spec.ViewSource{Table:"events"}}}
 h,err:=writer.New(component,reflect.TypeOf(EventsInput{}),reflect.TypeOf(EventsOutput{}),"patch");if err!=nil{t.Fatal(err)}
 input:=&EventsInput{};snapshot,err:=h.CaptureInput(context.Background(),input);if err!=nil{t.Fatal(err)};ctx,cancel:=context.WithCancel(context.Background());cancel();initCalls=0;validateCalls=0
 if _,err=h.Execute(ctx,rhandler.Invocation{Input:input,Snapshot:snapshot,Binder:batchBinder{}});err!=context.Canceled||initCalls!=0||validateCalls!=0{t.Fatal("cancellation did not precede business hooks",err)}
}
`
