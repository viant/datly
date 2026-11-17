package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
)

type EmbeddedInitValue struct {
	Calls   int
	Failure error
}

func (v *EmbeddedInitValue) Init(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	v.Calls++
	return v.Failure
}

func TestAnonymousEmbeddedInputInitialization(t *testing.T) {
	typ := reflect.StructOf([]reflect.StructField{
		{Name: "EmbeddedInitValue", Type: reflect.TypeFor[EmbeddedInitValue](), Anonymous: true},
		{Name: "Selection", Type: reflect.TypeFor[string]()},
	})
	contract := testRouteInput(t, typ)
	failure := errors.New("embedded validation failed")
	for _, expected := range []error{nil, failure} {
		input := reflect.New(typ)
		base := input.Elem().Field(0).Addr().Interface().(*EmbeddedInitValue)
		base.Failure = expected
		invoked := false
		_, err := New().Execute(t.Context(), Request{Input: contract, BoundInput: input.Interface(), Handler: rhandler.HandlerFunc(func(_ context.Context, invocation rhandler.Invocation) (any, error) {
			invoked = true
			if base.Calls != 1 {
				t.Fatalf("initializer calls=%d", base.Calls)
			}
			return invocation.Input, nil
		})})
		if !errors.Is(err, expected) || invoked != (expected == nil) || base.Calls != 1 {
			t.Fatalf("error=%v invoked=%v calls=%d", err, invoked, base.Calls)
		}
	}
}
