package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/x/shape"
)

type SeparateInputRules struct{ calls int }

func (r *SeparateInputRules) Init(_ context.Context, input any) error {
	r.calls++
	if r.calls != 1 {
		panic("lifecycle instance shared between requests")
	}
	v := reflect.ValueOf(input).Elem()
	v.FieldByName("Minimum").SetInt(v.FieldByName("Minimum").Int() + 1)
	return nil
}

func TestSeparateLifecycleOnFlatDynamicInput(t *testing.T) {
	typ := reflect.StructOf([]reflect.StructField{{Name: "Minimum", Type: reflect.TypeFor[int]()}})
	base := testRouteInput(t, typ)
	input, err := base.WithLifecycle(reflect.TypeFor[SeparateInputRules]())
	require.NoError(t, err)
	walk, err := shape.CompileRewrite(typ)
	require.NoError(t, err)
	metadata, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if field.Name == "Minimum" {
			return &shape.FieldMetadata{Name: field.Name, Tag: `json:"lowerBound"`}, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	input, err = input.WithInputRepresentation(metadata)
	require.NoError(t, err)
	for n := 0; n < 2; n++ {
		value := reflect.New(typ)
		value.Elem().Field(0).SetInt(7)
		_, err := New().Execute(t.Context(), Request{Input: input, BoundInput: value.Interface(), Handler: rhandler.HandlerFunc(func(_ context.Context, inv rhandler.Invocation) (any, error) {
			require.EqualValues(t, 8, reflect.ValueOf(inv.Input).Elem().Field(0).Int())
			return nil, nil
		})})
		require.NoError(t, err)
	}
	value := reflect.New(typ)
	require.Equal(t, value.Interface(), base.InitializerTarget(value.Interface()), "original route must remain unchanged")
}
