package transcribe

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTranscribedGoOutputInjectorFinalizer(t *testing.T) {
	verifyTranscribedSuccessFinalizer(t, HandlerGo, func(root, source string) string {
		source = strings.Replace(source, "func (o *EventsOutput) Finalize(context.Context) error {", "func (o *EventsOutput) Finalize(ctx context.Context, lookup func(context.Context, xhandler.Route) (xhandler.Binder, error)) error {", 1)
		source = strings.Replace(source, `lifecycleOrder = append(lifecycleOrder, "finalize")
	return nil`, `lifecycleOrder = append(lifecycleOrder, "finalize")
 binder, err := lookup(ctx, xhandler.Route{Method:"POST", URL:"/conditional"})
 if err != nil { return err }
 _, _, err = binder.Lookup(ctx, xhandler.ResultKey)
 return err`, 1)
		source = strings.Replace(source, "_ xhandler.Finalizer   =", "_ xhandler.InjectorFinalizer   =", 1)
		source = strings.Replace(source, `"init,commit,finalize"`, `"init,finalize,child,commit"`, 1)
		// This fixture's injector-specific transaction assertion is the first case;
		// plain success-path failure behavior stays covered by the original fixture.
		start := strings.Index(source, `	t.Run("is skipped after flush failure"`)
		end := strings.Index(source[start:], "\nvar (") + start
		source = source[:start] + "}\n" + source[end:]
		source = strings.Replace(source, "runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{{", `child := &spec.Component{Key:spec.Key{Kind:spec.KindComponent, Scope:"example.com/generated/events", Name:"Conditional"}, Routes:[]*spec.Route{{Method:"POST",Path:"/conditional"}}}
 childArtifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:child,InputType:reflect.TypeFor[conditionalInput](),OutputType:reflect.TypeFor[conditionalOutput]()})
 if err!=nil {t.Fatal(err)}
 runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{
 {Component:childArtifact.Component, Input:childArtifact.Input, OutputType:reflect.TypeFor[conditionalOutput](),DataSource:source,Handler:rhandler.HandlerFunc(func(ctx context.Context,inv rhandler.Invocation)(any,error){
  lifecycleOrder=append(lifecycleOrder,"child")
  input:=inv.Input.(*conditionalInput)
  if len(input.Data)!=1 {t.Fatalf("automatic input binding: %+v",input)}
  dependencies:=struct{DML xhandler.DML `+"`bind:\"kind=dml,required\"`"+`}{}
  if err:=inv.Binder.Bind(ctx,&dependencies);err!=nil{return nil,err}
  if err:=dependencies.DML.Execute("INSERT INTO EVENTS VALUES('child','conditional')");err!=nil{return nil,err}
  return &conditionalOutput{},nil
 })}, {`, 1)
		source += `
type conditionalInput struct { Data []*EventsView ` + "`parameter:\"Data,kind=caller_output,in=Data,required\"`" + ` }
type conditionalOutput struct{}
`
		// Put the authored hook in a real Go source file so package discovery
		// and linked contract validation see the supported callback signature.
		begin := strings.Index(source, "func (o *EventsOutput) Finalize(")
		endHook := strings.Index(source[begin:], "func resetLifecycle()") + begin
		hook := source[begin:endHook]
		source = source[:begin] + source[endHook:]
		begin = strings.Index(source, "var (")
		endVars := strings.Index(source[begin:], "\n)\n") + begin + len("\n)\n")
		vars := source[begin:endVars]
		source = source[:begin] + source[endVars:]
		hookSource := "package events\nimport (\"context\"; xhandler \"github.com/viant/xdatly/handler\")\n" + vars + hook
		if err := os.WriteFile(filepath.Join(root, "generated", "events_injector_hook.go"), []byte(hookSource), 0644); err != nil {
			t.Fatal(err)
		}
		source = strings.Replace(source, "\"context\"", "\"context\"\n \"os\"", 1)
		source += `
func TestDiscoveredOutputInjectorSignature(t *testing.T) {
 cwd,err:=os.Getwd();if err!=nil{t.Fatal(err)}
 sources,err:=bootstrap.DiscoverComponentsFromPackages(context.Background(),filepath.Dir(cwd),[]string{"example.com/generated/..."},nil)
 if err!=nil{t.Fatal(err)}
 if len(sources)!=1{t.Fatalf("discovered %d components",len(sources))}
 if err=sources[0].ValidateContractTypes(reflect.TypeFor[EventsInput](),reflect.TypeFor[EventsOutput]());err!=nil{t.Fatal(err)}
 if !reflect.TypeFor[*EventsOutput]().Implements(reflect.TypeFor[xhandler.InjectorFinalizer]()){t.Fatal("discovered output lost injector lifecycle")}
}
`
		return source
	})
}
