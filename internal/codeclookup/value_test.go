package codeclookup

import (
	"context"
	"errors"
	"testing"
)

func TestOptionalMissingValueDoesNotBecomeAnError(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "invocation")
	value, err := Value(ctx, "Mode", map[string]bool{"Mode": true}, func(actual context.Context, name string) (any, bool, error) {
		if actual != ctx || name != "Mode" {
			t.Fatal("lookup identity changed")
		}
		return 123, false, nil
	})
	if err != nil || value != nil {
		t.Fatalf("omitted optional = %v, %v", value, err)
	}
}

func TestValueRetainsExplicitZeroAndFalse(t *testing.T) {
	for _, input := range []any{0, false, "", nil} {
		value, err := Value(context.Background(), "Mode", map[string]bool{"Mode": true}, func(context.Context, string) (any, bool, error) { return input, true, nil })
		if err != nil || value != input {
			t.Fatalf("explicit value = %v, %v", value, err)
		}
	}
}

func TestUnknownRequiredAndResolverFailuresRemainErrors(t *testing.T) {
	for _, name := range []string{"Unknown", "Required"} {
		_, err := Value(context.Background(), name, map[string]bool{"Optional": true}, func(context.Context, string) (any, bool, error) { return nil, false, nil })
		if err == nil {
			t.Fatalf("missing %s accepted", name)
		}
	}
	cause := errors.New("conversion failed")
	_, err := Value(context.Background(), "Optional", map[string]bool{"Optional": true}, func(context.Context, string) (any, bool, error) { return nil, false, cause })
	if !errors.Is(err, cause) {
		t.Fatalf("resolver cause lost: %v", err)
	}
}
