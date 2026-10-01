package engine

import (
	"context"
	"errors"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type independentInput struct{}
type independentOutput struct{}
type independentInjectorOutput struct{}

func (*independentInjectorOutput) Finalize(context.Context, xhandler.InjectorLookup) error {
	return nil
}

type independentOutcomeHandler struct{ rhandler.TypedHandler }

func (independentOutcomeHandler) FinalizeOutcome(context.Context, rhandler.Invocation, any, xhandler.Outcome) error {
	return nil
}
func (independentOutcomeHandler) SupportsIndependentChildTransactions() bool { return true }

type independentSource struct{}

func (independentSource) Open(context.Context) (xhandler.Data, error) {
	panic("ownership guard must run before New")
}

func TestIndependentChildOwnershipGuards(t *testing.T) {
	contract := custom.New[independentInput, independentOutput](xhandler.ContractFunc[independentInput, independentOutput](func(context.Context, xhandler.Session, *independentInput, *independentOutput) error { return nil }))
	for _, tc := range []struct {
		description string
		input       Request
		inherited   bool
		expected    string
	}{
		{"source-less custom adapter retains independent eligibility", Request{Handler: contract}, false, ""},
		{"root source is forbidden before effects", Request{Handler: contract, DataSource: independentSource{}}, false, "root has a data source"},
		{"caller unit cannot be escaped", Request{Handler: contract}, true, "caller has an inherited managed unit"},
		{"outcome ownership cannot mix with independent children", Request{Handler: independentOutcomeHandler{contract}}, false, "root owns outcome finalization"},
		{"injector finalizer ownership cannot mix", Request{Handler: contract, OutputType: reflect.TypeFor[independentInjectorOutput]()}, false, "root owns injector finalization"},
		{"root completion ownership cannot mix", Request{Handler: contract, Completion: func(xhandler.Outcome) {}}, false, "root owns completion observation"},
		{"root sequence strategy is not source-less", Request{Handler: contract, SequenceStrategy: "native"}, false, "root declares a sequence strategy"},
		{"ordinary handler does not opt into custom protocol", Request{Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { return nil, nil })}, false, "handler is not a custom orchestration contract"},
	} {
		t.Run(tc.description, func(t *testing.T) {
			ctx := t.Context()
			if tc.inherited {
				ctx = withDataScope(ctx, neutralDataScope())
			}
			err := validateIndependentChildren(ctx, tc.input)
			if tc.expected == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var failure *dexec.IndependentChildTransactionError
			if !errors.As(err, &failure) || failure.Reason != tc.expected {
				t.Fatalf("expected typed reason %q, got %v", tc.expected, err)
			}
		})
	}
}
