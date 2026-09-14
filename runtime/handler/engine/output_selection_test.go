package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

type selectionTestFilter string

func (f selectionTestFilter) ExcludePath(_ []string, name string) bool { return string(f) != name }

type selectionTestOutput struct{ finalize func(context.Context) error }

func (o *selectionTestOutput) Finalize(ctx context.Context, _ xhandler.InjectorLookup) error {
	return o.finalize(ctx)
}

type selectionHandler func(context.Context, rhandler.Invocation) (any, error)

func (f selectionHandler) Execute(ctx context.Context, i rhandler.Invocation) (any, error) {
	return f(ctx, i)
}

func TestOutputSelectionNestedAndInjectorFinalizerIsolation(t *testing.T) {
	input := testRouteInput(t, reflect.TypeFor[struct{}]())
	ctx := dexec.CaptureOutputSelection(context.Background())
	child := func(ctx context.Context) {
		_, err := New().Execute(ctx, Request{Input: input, Handler: selectionHandler(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
			result := &selectionTestOutput{finalize: func(context.Context) error { return nil }}
			dexec.PublishOutputSelection(ctx, result, selectionTestFilter("child"))
			return result, nil
		})})
		require.NoError(t, err)
	}
	output := &selectionTestOutput{finalize: func(ctx context.Context) error {
		child(ctx)
		// A direct read in the finalizer also cannot replace the handler's mask.
		dexec.PublishOutputSelection(ctx, &selectionTestOutput{}, selectionTestFilter("finalizer"))
		return nil
	}}
	result, err := New().Execute(ctx, Request{Input: input, Handler: selectionHandler(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
		child(ctx)
		dexec.PublishOutputSelection(ctx, output, selectionTestFilter("root"))
		return output, nil
	})})
	require.NoError(t, err)
	require.Same(t, output, result)
	filter := dexec.SelectedOutputFields(ctx, result)
	require.NotNil(t, filter)
	require.False(t, filter.ExcludePath(nil, "root"))
	require.True(t, filter.ExcludePath(nil, "child"))
}

func TestOutputSelectionDoesNotReusePriorInvocation(t *testing.T) {
	input := testRouteInput(t, reflect.TypeFor[struct{}]())
	ctx := dexec.CaptureOutputSelection(context.Background())
	first := &struct{ ID int }{}
	_, err := New().Execute(ctx, Request{Input: input, Handler: selectionHandler(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
		dexec.PublishOutputSelection(ctx, first, selectionTestFilter("ID"))
		return first, nil
	})})
	require.NoError(t, err)
	require.NotNil(t, dexec.SelectedOutputFields(ctx, first))
	_, err = New().Execute(ctx, Request{Input: input, Handler: selectionHandler(func(context.Context, rhandler.Invocation) (any, error) { return first, nil })})
	require.NoError(t, err)
	require.Nil(t, dexec.SelectedOutputFields(ctx, first))
}
