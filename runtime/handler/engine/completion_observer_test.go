package engine

import (
	"context"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type completionObservedOutput struct{ calls int }

func (o *completionObservedOutput) Finalize(context.Context, error) error { o.calls++; return nil }
func TestCompletionObserverPreservesOutputLifecycle(t *testing.T) {
	output := &completionObservedOutput{}
	observed := 0
	result, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeOf(struct{}{})), BoundInput: &struct{}{}, Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { return output, nil }), Completion: func(outcome xhandler.Outcome) {
		observed++
		if outcome.State() != xhandler.TransactionNone {
			t.Errorf("outcome=%+v", outcome)
		}
	}})
	if err != nil || result != output || output.calls != 1 || observed != 1 {
		t.Fatalf("result=%v err=%v finalizers=%d observations=%d", result, err, output.calls, observed)
	}
}
