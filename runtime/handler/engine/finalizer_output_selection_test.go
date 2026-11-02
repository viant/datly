package engine

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
)

// This structural assertion exposes the missing capability without requiring a
// new SDK symbol merely to demonstrate the existing runtime failure.
type finalizerSelectionProvider interface {
	OutputSelection(context.Context) (dexec.OutputFieldFilter, bool, error)
}
type finalizerSelectionOutput struct {
	inspect func(context.Context, finalizerSelectionProvider) error
}

func (*finalizerSelectionOutput) RequiresOutputSelection() bool { return true }
func (o *finalizerSelectionOutput) Finalize(ctx context.Context, invoker xhandler.ComponentInvoker) error {
	provider, ok := invoker.(finalizerSelectionProvider)
	if !ok {
		return fmt.Errorf("finalizer cannot inspect effective output selection")
	}
	return o.inspect(ctx, provider)
}

func TestFinalizerCanInspectItsOwnOutputSelection(t *testing.T) {
	type useCase struct {
		description string
		filter      dexec.OutputFieldFilter
	}
	for _, tc := range []useCase{
		{description: "unrestricted selection"},
		{description: "restricted relation selection", filter: selectionTestFilter("Children")},
	} {
		t.Run(tc.description, func(t *testing.T) {
			var retained finalizerSelectionProvider
			out := &finalizerSelectionOutput{inspect: func(ctx context.Context, p finalizerSelectionProvider) error {
				retained = p
				actual, available, err := p.OutputSelection(ctx)
				require.NoError(t, err)
				require.True(t, available, "nil unrestricted mask must differ from unavailable evidence")
				require.Equal(t, tc.filter, actual)
				return nil
			}}
			result, err := New().Execute(context.Background(), Request{
				Input:      testRouteInput(t, reflect.TypeFor[struct{}]()),
				OutputType: reflect.TypeFor[finalizerSelectionOutput](),
				Handler: selectionHandler(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
					dexec.PublishOutputSelection(ctx, out, tc.filter)
					return out, nil
				}),
			})
			require.NoError(t, err)
			require.Same(t, out, result)
			_, _, err = retained.OutputSelection(context.Background())
			require.ErrorContains(t, err, "closed")
		})
	}
}

func TestFinalizerSelectionAvailabilityAndIsolation(t *testing.T) {
	type useCase struct {
		description string
		publish     bool
		wrongType   bool
		fail        bool
		nested      bool
		available   bool
	}
	for _, tc := range []useCase{
		{description: "no reader evidence"},
		{description: "wrong result type", publish: true, wrongType: true},
		{description: "failed read", publish: true, fail: true},
		{description: "nested same-type read", publish: true, nested: true, available: true},
	} {
		t.Run(tc.description, func(t *testing.T) {
			t.Parallel()
			input := testRouteInput(t, reflect.TypeFor[struct{}]())
			out := &finalizerSelectionOutput{}
			out.inspect = func(ctx context.Context, p finalizerSelectionProvider) error {
				if tc.nested {
					child := &finalizerSelectionOutput{inspect: func(ctx context.Context, p finalizerSelectionProvider) error {
						mask, ok, err := p.OutputSelection(ctx)
						require.NoError(t, err)
						require.True(t, ok)
						require.Equal(t, selectionTestFilter("child"), mask)
						return nil
					}}
					_, err := New().Execute(ctx, Request{Input: input, OutputType: reflect.TypeFor[finalizerSelectionOutput](), Handler: selectionHandler(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
						dexec.PublishOutputSelection(ctx, child, selectionTestFilter("child"))
						return child, nil
					})})
					require.NoError(t, err)
				}
				mask, ok, err := p.OutputSelection(ctx)
				require.NoError(t, err)
				require.Equal(t, tc.available, ok)
				if ok {
					require.Equal(t, selectionTestFilter("parent"), mask)
				} else {
					require.Nil(t, mask)
				}
				return nil
			}
			_, err := New().Execute(context.Background(), Request{Input: input, OutputType: reflect.TypeFor[finalizerSelectionOutput](), Handler: selectionHandler(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
				if tc.publish {
					var value any = out
					if tc.wrongType {
						value = &struct{}{}
					}
					dexec.PublishOutputSelection(ctx, value, selectionTestFilter("parent"))
				}
				if tc.fail {
					return out, fmt.Errorf("reader failed")
				}
				return out, nil
			})})
			if tc.fail {
				require.ErrorContains(t, err, "reader failed")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestFinalizerSelectionRejectsInvalidCallContext(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	type useCase struct {
		description string
		ctx         context.Context
		expected    string
	}
	for _, tc := range []useCase{{"nil context", nil, "call context is required"}, {"canceled context", canceled, "context canceled"}} {
		t.Run(tc.description, func(t *testing.T) {
			root := context.Background()
			lease := &lifecycleScope{data: neutralDataScope(root), ctx: root, phase: "invoker finalizer"}
			filter, available, err := lease.OutputSelection(tc.ctx)
			require.ErrorContains(t, err, tc.expected)
			require.Nil(t, filter)
			require.False(t, available)
		})
	}
}
