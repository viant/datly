package engine

import (
	"context"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

type injectorTestOutput struct{ calls int }

func (o *injectorTestOutput) Finalize(context.Context, xhandler.InjectorLookup) error {
	o.calls++
	return nil
}
func TestInjectorFinalizerPreservesOutcomeHandlerOwnership(t *testing.T) {
	output := &injectorTestOutput{}
	completions := 0
	handler := &outcomeAwareHandler{
		execute: func(context.Context, rhandler.Invocation) (any, error) { return output, nil },
		finalize: func(_ context.Context, _ rhandler.Invocation, got any, outcome xhandler.Outcome) error {
			completions++
			if got != output || outcome.Error != nil {
				t.Errorf("completion: %v %+v", got, outcome)
			}
			return nil
		},
	}
	result, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), OutputType: reflect.TypeFor[injectorTestOutput](), Handler: handler})
	if err != nil || result != output || output.calls != 0 || completions != 1 {
		t.Fatalf("result=%v err=%v output=%d outcome=%d", result, err, output.calls, completions)
	}
}
