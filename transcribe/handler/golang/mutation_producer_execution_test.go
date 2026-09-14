package golang

import (
	"strings"
	"testing"
)

// Velty invokes the canonical native mutation adapter; it does not reimplement
// phases, classification, receipts or transaction completion in template code.
func TestRelationProducerSharedProgramVeltySQLite(t *testing.T) {
	for _, self := range []bool{false, true} {
		layout := "relation"
		if self {
			layout = "self"
		}
		t.Run(layout, func(t *testing.T) {
			for _, mode := range []string{"absent child", "produced Go failure", "parent update", "supplied nil", "altered topology"} {
				t.Run(mode, func(t *testing.T) {
					(relationProducerFixture{self: self, composite: mode != "parent update", velty: true, mode: mode}).run(t)
				})
			}
		})
	}
}

func TestRelationProducerCallerTransactionSQLite(t *testing.T) {
	for _, self := range []bool{false, true} {
		layout := "relation"
		if self {
			layout = "self"
		}
		t.Run(layout, func(t *testing.T) {
			(relationProducerFixture{self: self, composite: true, callerTx: true, mode: "absent child"}).run(t)
		})
	}
}

func (f relationProducerFixture) veltySource(source string) string {
	source = strings.NewReplacer(
		`"context";`, `rhandler "github.com/viant/datly/runtime/handler";vhandler "github.com/viant/datly/runtime/handler/velty";"context";`,

		`Handler:mutation.New[Input,Output](definition)`, `Handler:sharedVelty{TypedHandler:mutation.New[Input,Output](definition)}`,
	).Replace(source)
	source += `
type programRunner struct{run func()(bool,error)}
func(r *programRunner)Run()(bool,error){return r.run()}
type veltyInput struct{Runner *programRunner}
type veltyOutput struct{Done bool}
type sharedVelty struct{rhandler.TypedHandler}
func(h sharedVelty)RequiresReadMetadata()bool{return true}
func(h sharedVelty)CaptureInput(ctx context.Context,input any)(any,error){return h.TypedHandler.(rhandler.InputCapturer).CaptureInput(ctx,input)}
func(h sharedVelty)FinalizeOutcome(ctx context.Context,invocation rhandler.Invocation,result any,outcome handler.Outcome)error{return h.TypedHandler.(rhandler.OutcomeFinalizer).FinalizeOutcome(ctx,invocation,result,outcome)}
func(h sharedVelty)Execute(ctx context.Context,invocation rhandler.Invocation)(any,error){
 var result any
 runner:=&programRunner{run:func()(bool,error){var err error;result,err=h.TypedHandler.Execute(ctx,invocation);return err==nil,err}}
 template,err:=vhandler.New[veltyInput,veltyOutput](vhandler.Config{Template:"#set($Output.Done = $Input.Runner.Run())"});if err!=nil{return nil,err}
 scope:=invocation;scope.Input=&veltyInput{Runner:runner}
 _,err=template.Execute(ctx,scope);return result,err
}
`
	return source
}

func (f relationProducerFixture) transactionSource(source string) string {
	source = strings.Replace(source, ` _,err=engine.New().Execute`, ` suppliedTx,err:=h.DB.BeginTx(ctx,nil);if err!=nil{t.Fatal(err)};defer suppliedTx.Rollback()
 _,err=engine.New().Execute`, 1)
	source = strings.Replace(source, `DataSource:dml.Source{DB:h.DB}`, `DataSource:dml.Source{DB:h.DB,Tx:suppliedTx}`, 1)
	source = strings.Replace(source, ` if mode=="incomplete key"{`, ` if suppliedTx!=nil{if err!=nil{t.Fatal(err)};if len(outcomes)!=1||outcomes[0].CommitConfirmed()||outcomes[0].State()!=handler.TransactionCallerPending||queueCalls!=2||writes.Load()!=2{t.Fatalf("caller-owned mutation outcome: %+v queue=%d writes=%d",outcomes,queueCalls,writes.Load())}
 var n int;if err:=suppliedTx.QueryRowContext(ctx,"SELECT COUNT(*) FROM nodes").Scan(&n);err!=nil||n!=3{t.Fatalf("pending graph not in caller transaction: %d %v",n,err)}
 if err:=suppliedTx.Rollback();err!=nil{t.Fatal(err)};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS n FROM nodes"},[]struct{N int}{{1}});return}
 if mode=="incomplete key"{`, 1)
	return source
}
