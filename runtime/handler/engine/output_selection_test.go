package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	structjson "github.com/viant/structology/encoding/json"
	xhandler "github.com/viant/xdatly/handler"
)

type selectionTestFilter string

func (f selectionTestFilter) ExcludePath(_ []string, name string) bool { return string(f) != name }

type selectionTestOutput struct{ finalize func(context.Context) error }

func (o *selectionTestOutput) Finalize(ctx context.Context, _ xhandler.InjectorLookup) error {
	return o.finalize(ctx)
}

type delegatedSelectionRow struct {
	Unused  int      `json:"unused"`
	Count   int      `json:"count"`
	Enabled bool     `json:"enabled"`
	Label   string   `json:"label"`
	Price   *float64 `json:"price"`
}
type delegatedSelectionOutput struct {
	Rows   []delegatedSelectionRow `json:"data"`
	Status string                  `json:"status"`
}
type delegatedSelectionWrapper delegatedSelectionOutput

func TestExplicitOutputSelectionDelegation(t *testing.T) {
	input := testRouteInput(t, reflect.TypeFor[struct{}]())
	for _, fail := range []bool{false, true} {
		ctx := dexec.CaptureOutputSelection(context.Background())
		filter, err := structjson.NewFieldFilter(reflect.TypeFor[delegatedSelectionOutput](), []structjson.FieldSelection{{Path: []string{"Rows"}, Fields: []string{"Count", "Enabled", "Label", "Price"}}})
		require.NoError(t, err)
		reader := selectionHandler(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
			out := &delegatedSelectionOutput{Rows: []delegatedSelectionRow{{Unused: 123}}, Status: "ok"}
			dexec.PublishOutputSelection(ctx, out, filter)
			if fail {
				return out, errors.New("failed child")
			}
			return out, nil
		})
		wrapper := selectionHandler(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
			child := dexec.CaptureChildOutputSelection(ctx)
			value, err := New().Execute(child, Request{Input: input, Handler: reader})
			if err != nil {
				return nil, err
			}
			out := (*delegatedSelectionWrapper)(value.(*delegatedSelectionOutput))
			dexec.PublishOutputSelection(ctx, out, dexec.SelectedOutputFields(child, value))
			// An unrelated child, even of the same output type, must not replace it.
			_, err = New().Execute(ctx, Request{Input: input, Handler: selectionHandler(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
				other := &delegatedSelectionWrapper{}
				dexec.PublishOutputSelection(ctx, other, selectionTestFilter("unused"))
				return other, nil
			})})
			return out, err
		})
		result, err := New().Execute(ctx, Request{Input: input, Handler: selectionHandler(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
			child := dexec.CaptureChildOutputSelection(ctx)
			value, err := New().Execute(child, Request{Input: input, Handler: wrapper})
			if err == nil {
				dexec.PublishOutputSelection(ctx, value, dexec.SelectedOutputFields(child, value))
			}
			return value, err
		})})
		if fail {
			require.Error(t, err)
			require.Nil(t, dexec.SelectedOutputFields(ctx, &delegatedSelectionWrapper{}))
			continue
		}
		require.NoError(t, err)
		selected := dexec.SelectedOutputFields(ctx, result)
		require.NotNil(t, selected)
		encoded, err := structjson.MarshalStandard(result, structjson.WithPathFieldExcluder(selected))
		require.NoError(t, err)
		require.JSONEq(t, `{"data":[{"count":0,"enabled":false,"label":"","price":null}],"status":"ok"}`, string(encoded))
	}
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
