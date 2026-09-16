package golang

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestOptionalHookFailuresAreNotRetried(t *testing.T) {
	semantic := rootSemanticPlan(plan.OperationPost, false)
	semantic.Root.Entity = &plan.EntityPlan{Type: spec.TypeRef{Name: "Record"}, MarkerField: "Has", MarkerPointer: true, Keys: semantic.Root.Keys, Hooks: spec.TypeRef{Name: "Hooks"}, Fields: []plan.EntityField{{Name: "Id", Type: spec.TypeRef{Name: "*int64"}, Identity: true, Writable: true}}}
	types := rootRecordTypes(semantic, "[]*Record", "")
	config := Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "Input", OutputType: "Output", Records: types}
	hooks, err := MutationHookSupport(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	source := strings.NewReplacer("{{HOOKS}}", hooks.TypeName, "{{FRAMES}}", hooks.FramesType, "{{FRAME}}", hooks.Roles[0].FrameType, "{{ROLE}}", hooks.Roles[0].Field).Replace(optionalHookFailureFixture)
	runEntitySyncFixture(t, semantic, types, source, hooks.File)
}

const optionalHookFailureFixture = `package events
import("context";"errors";"testing";"github.com/viant/xdatly/handler")
type Marker struct{Id bool}
type Record struct{Id *int64;Has *Marker}
type Input struct{Events []*Record}
type Output struct{Data []*Record}
var callbackError=errors.New("callback failed")
type Hooks struct{initialized,sequences,queues int;fail string}
func(h *Hooks)Init(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{h.initialized++;return nil}
func(h *Hooks)Validate(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{if h.initialized!=2{return callbackError};return nil}
func(h *Hooks)AfterSequence(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{h.sequences++;if h.fail=="sequence"{return callbackError};return nil}
func(h *Hooks)AfterQueue(context.Context,*Record,handler.LifecycleContext[Record,handler.NoParent,Output])error{h.queues++;if h.fail=="queue"{return callbackError};return nil}
func TestOptionalFailure(t *testing.T){
 for _,phase:=range []string{"sequence","queue"}{t.Run(phase,func(t *testing.T){
  ctx:=context.Background();hooks:=&{{HOOKS}}{}
  frames:=&{{FRAMES}}{ {{ROLE}}:[]*{{FRAME}}{ {Entity:&Record{}},{Entity:&Record{}} } }
  if err:=hooks.Prepare(ctx,nil,&Output{});err!=nil{t.Fatal(err)}
  if err:=hooks.AfterSequence(ctx,frames);err==nil{t.Fatal("AfterSequence before Validate accepted")}
  if err:=hooks.Init(ctx,frames);err!=nil{t.Fatal(err)};if err:=hooks.Validate(ctx,frames);err!=nil{t.Fatal(err)}
  hooks.hook0.fail=phase
  err:=hooks.AfterSequence(ctx,frames)
  if phase=="sequence"{
   if !errors.Is(err,callbackError)||hooks.hook0.sequences!=1{t.Fatalf("error=%v calls=%d",err,hooks.hook0.sequences)}
   if err:=hooks.AfterQueue(ctx,frames);err==nil||hooks.hook0.queues!=0{t.Fatal("AfterQueue ran after failed sequence callback")}
   if err:=hooks.AfterSequence(ctx,frames);err==nil||hooks.hook0.sequences!=1{t.Fatal("failed callback retried")};return
  }
  if err!=nil||hooks.hook0.sequences!=2{t.Fatalf("sequence=%v",err)}
  if err:=hooks.AfterQueue(ctx,frames);!errors.Is(err,callbackError)||hooks.hook0.queues!=1{t.Fatalf("queue=%v",err)}
  if err:=hooks.AfterQueue(ctx,frames);err==nil||hooks.hook0.queues!=1{t.Fatal("failed queue callback retried")}
 })}
}
`
