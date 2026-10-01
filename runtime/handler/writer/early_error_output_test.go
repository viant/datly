package writer

import (
	"context"
	"errors"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

type earlyWriterOutput struct {
	calls     int
	cause     error
	projected error
}

func (o *earlyWriterOutput) Finalize(_ context.Context, cause error) error {
	o.calls++
	o.cause = cause
	return o.projected
}

func TestWriterEarlyOutcomeDispatchesOnlyErrorAwareOutputWithOriginalCause(t *testing.T) {
	cause := errors.New("dependency binding failed")
	projected := errors.New("application response")
	for _, tc := range []struct {
		desc          string
		input         error
		expectedCalls int
	}{{"binding failure", cause, 1}, {"success without program", nil, 0}} {
		t.Run(tc.desc, func(t *testing.T) {
			output := &earlyWriterOutput{projected: projected}
			err := (&Handler{}).FinalizeOutcome(context.Background(), rhandler.Invocation{}, output, xhandler.Outcome{Error: tc.input})
			if output.calls != tc.expectedCalls || output.cause != tc.input {
				t.Fatalf("callback calls=%d cause=%v", output.calls, output.cause)
			}
			if tc.expectedCalls == 1 && err != projected || tc.expectedCalls == 0 && err != nil {
				t.Fatalf("application projection=%v", err)
			}
		})
	}
}
