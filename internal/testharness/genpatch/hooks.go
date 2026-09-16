package genpatch

const RootHooks = `package roothooks
import (
 "context"
 "fmt"
 requests "github.com/viant/datly/genfixture/requests"
 responses "github.com/viant/datly/genfixture/responses"
 rows "github.com/viant/datly/genfixture/entities"
 h "github.com/viant/xdatly/handler"
)
type Hooks struct { Input *requests.OrdersInput ` + "`bind:\"kind=input\"`" + ` }
var HookObservedOriginal,LookupRead bool
var Calls,Completions int
const Version=1
func(hooks *Hooks)Init(_ context.Context,entity *rows.Order,state h.LifecycleContext[rows.Order,h.NoParent,responses.OrdersOutput])error{
 Calls++
 if hooks.Input==nil{return fmt.Errorf("authored root hook input missing")}
 if entity.Has!=nil && entity.Has.KindId && entity.KindId==nil{
  if len(hooks.Input.CurrentKinds)!=0{return fmt.Errorf("explicit null acquired previous lookup")}
 }else{
  current:=hooks.Input.CurrentKinds
  if len(current)!=1||current[0].Id==nil||*current[0].Id!=7||current[0].Name==nil||*current[0].Name!="standard"{return fmt.Errorf("authored root hook lookup missing")}
  LookupRead=true
 }
 if state.Original!=nil && state.Original.Has("Start")&&!state.Original.Has("End"){
  if entity.End==nil||entity.End.IsZero(){return fmt.Errorf("invariant backfill missing")};HookObservedOriginal=true
 }
 return nil
}
func(*Hooks)Validate(context.Context,*rows.Order,h.LifecycleContext[rows.Order,h.NoParent,responses.OrdersOutput])error{return nil}
func(*Hooks)Finalize(_ context.Context,_ *requests.OrdersInput,_ *responses.OrdersOutput,outcome h.Outcome)error{if outcome.CommitConfirmed(){Completions++};return nil}
`

const ChildHooks = `package childhooks
import (
 "context"
 "fmt"
 rows "github.com/viant/datly/genfixture/entities"
 items "github.com/viant/datly/genfixture/items"
 responses "github.com/viant/datly/genfixture/responses"
 h "github.com/viant/xdatly/handler"
)
type Hooks struct{}
var Calls int
const Version=1
func(*Hooks)Init(_ context.Context,_ *items.Item,state h.LifecycleContext[items.Item,rows.Order,responses.OrdersOutput])error{Calls++;if state.Parent==nil{return fmt.Errorf("typed child parent missing")};return nil}
func(*Hooks)Validate(context.Context,*items.Item,h.LifecycleContext[items.Item,rows.Order,responses.OrdersOutput])error{return nil}
`
