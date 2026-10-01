package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

type earlyOutcomeHandler struct {
	outcomeAwareHandler
	enabled bool
}

func (h *earlyOutcomeHandler) EarlyErrorOutputEnabled() bool     { return h.enabled }
func (*earlyOutcomeHandler) RequiresPreBindingTransaction() bool { return true }

func TestOutcomeAdapterEarlyOutputRequiresOptInAndFinalizesOnceAfterCleanup(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			data := &completedEarlyData{}
			sink := &earlyOutputSink{}
			executions, finalizations := 0, 0
			h := &earlyOutcomeHandler{enabled: enabled}
			h.execute = func(context.Context, rhandler.Invocation) (any, error) {
				executions++
				return nil, errors.New("handler must not execute")
			}
			h.finalize = func(ctx context.Context, invocation rhandler.Invocation, result any, outcome xhandler.Outcome) error {
				finalizations++
				if data.completes != 1 || invocation.Snapshot != nil {
					t.Fatal("early callback preceded cleanup or fabricated an execution snapshot")
				}
				var binding *bindly.BindingError
				if !errors.As(outcome.Error, &binding) || binding.Path != "Name" || binding.StatusCode() != 418 {
					t.Fatalf("normal input binding cause lost: %v", outcome.Error)
				}
				if result == nil {
					return nil
				}
				return result.(*earlyOutput).Finalize(ctx, outcome.Error)
			}
			result, err := New().Execute(context.Background(), Request{
				Input:      testRouteInput(t, reflect.TypeFor[earlyOutputInput]()),
				OutputType: reflect.TypeFor[earlyOutput](), OutputCapabilities: earlyOutputPlan(t),
				DataSource: &staticDataSource{data: data}, Handler: h,
				Capabilities: rhandler.InvocationCapabilities{Logger: sink},
			})
			if err == nil || executions != 0 || finalizations != 1 || data.completes != 1 {
				t.Fatalf("execution/finalization/cleanup=%d/%d/%d error=%v", executions, finalizations, data.completes, err)
			}
			if !enabled {
				if result != nil || sink.calls != 0 {
					t.Fatal("disabled adapter constructed error output")
				}
				return
			}
			output, ok := result.(*earlyOutput)
			if !ok || output.calls != 1 || output.Logger != sink || sink.calls != 1 {
				t.Fatalf("static logger or once-only callback mismatch: %T", result)
			}
		})
	}
}
