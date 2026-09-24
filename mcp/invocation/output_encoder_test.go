package invocation

import (
	"context"
	"errors"
	"testing"
)

func TestExecutionUsesInjectedOutputEncoder(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "invocation")
	value := &struct{ ID int }{ID: 7}
	want := errors.New("encoding failed")
	execution := &Execution{
		value: value, encodingContext: ctx,
		encodeOutput: func(actual context.Context, output any) ([]byte, error) {
			if actual.Value(contextKey{}) != "invocation" || output != value {
				t.Fatal("encoding adapter lost invocation context or output")
			}
			return nil, want
		},
	}
	if _, err := execution.Payload(); !errors.Is(err, want) {
		t.Fatalf("encoding error not preserved: %v", err)
	}
	execution.encodeOutput = func(context.Context, any) ([]byte, error) {
		return []byte(`{"id":7}`), nil
	}
	data, err := execution.Payload()
	if err != nil || string(data) != `{"id":7}` {
		t.Fatalf("encoded payload=%s err=%v", data, err)
	}
}

func TestRawResponseBypassesInjectedOutputEncoder(t *testing.T) {
	want := []byte{0, 255, 1}
	execution := &Execution{
		value: &singleBodyResponse{payload: want, status: 200},
		encodeOutput: func(context.Context, any) ([]byte, error) {
			t.Fatal("raw response must not be reinterpreted by the output encoder")
			return nil, nil
		},
	}
	data, err := execution.Payload()
	if err != nil || string(data) != string(want) {
		t.Fatalf("raw payload=%v err=%v", data, err)
	}
}
