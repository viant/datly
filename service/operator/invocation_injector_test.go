package operator

import (
	"context"
	"testing"

	"github.com/viant/datly/service/executor/uow"
	xstate "github.com/viant/xdatly/handler/state"
)

type recordingInjector struct {
	frames []*uow.Frame
}

func (r *recordingInjector) Into(ctx context.Context, _ interface{}, _ ...xstate.Option) error {
	return r.record(ctx)
}

func (r *recordingInjector) Bind(ctx context.Context, _ interface{}, _ ...xstate.Option) error {
	return r.record(ctx)
}

func (r *recordingInjector) Value(ctx context.Context, _ string) (interface{}, bool, error) {
	return nil, false, r.record(ctx)
}

func (r *recordingInjector) ValuesOf(ctx context.Context, _ interface{}) (map[string]interface{}, error) {
	return nil, r.record(ctx)
}

func (r *recordingInjector) record(ctx context.Context) error {
	_, frame, ok := uow.FromContext(ctx)
	if !ok {
		return uow.ErrTransactionAccess
	}
	r.frames = append(r.frames, frame)
	_, err := uow.ReserveBindingOrder(ctx)
	return err
}

func TestInvocationInjectorCreatesFreshFramePerOperation(t *testing.T) {
	ctx, scope, root := uow.NewRoot(context.Background(), "root")
	delegate := &recordingInjector{}
	injector := &invocationInjector{ctx: ctx, name: "GET /child", delegate: delegate}
	if err := injector.Bind(ctx, &struct{}{}); err != nil {
		t.Fatal(err)
	}
	if err := injector.Bind(ctx, &struct{}{}); err != nil {
		t.Fatal(err)
	}
	if len(delegate.frames) != 2 || delegate.frames[0] == delegate.frames[1] {
		t.Fatalf("expected two distinct invocation frames, got %v", delegate.frames)
	}
	root.Seal()
	if err := scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
}
