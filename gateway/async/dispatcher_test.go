package async

import (
	"context"
	"github.com/viant/datly/runtime/jobs"
	"testing"
)

func TestDispatcherRejectsMissingJobService(t *testing.T) {
	// The application adapter must preserve the constructor's concrete-service
	// nil rejection when the argument is now carried through JobHandler.
	for _, handler := range []JobHandler{nil, (*jobs.Service)(nil)} {
		if _, err := NewDispatcher(nil, handler); err == nil {
			t.Fatal("missing job handler accepted")
		}
	}
}

type testJobHandler struct{}

func (*testJobHandler) HandleJob(context.Context, *jobs.Event) (any, error) { return nil, nil }

type testJobHandlerFunc func(context.Context, *jobs.Event) (any, error)

func (f testJobHandlerFunc) HandleJob(ctx context.Context, event *jobs.Event) (any, error) {
	return f(ctx, event)
}

type testJobHandlerMap map[string]any

func (testJobHandlerMap) HandleJob(context.Context, *jobs.Event) (any, error) { return nil, nil }

func TestDispatcherRejectsTypedNilImplementations(t *testing.T) {
	for _, handler := range []JobHandler{(*testJobHandler)(nil), testJobHandlerFunc(nil), testJobHandlerMap(nil)} {
		if _, err := NewDispatcher(nil, handler); err == nil {
			t.Fatalf("typed-nil %T accepted", handler)
		}
	}
	for _, handler := range []JobHandler{&testJobHandler{}, testJobHandlerFunc(func(context.Context, *jobs.Event) (any, error) { return nil, nil }), testJobHandlerMap{}} {
		if _, err := NewDispatcher(nil, handler); err != nil {
			t.Fatalf("valid %T rejected: %v", handler, err)
		}
	}
}
