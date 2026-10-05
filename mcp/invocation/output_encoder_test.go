package invocation

import (
	"context"
	"errors"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
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

func TestExplicitErrorOutputEncoderPlumbingAndFailure(t *testing.T) {
	target := exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "Error"}, Route: spec.RouteRef{Method: "POST", Path: "/error"}}
	body := &struct{ Items []int }{}
	captured := &captureInvoker{result: map[string]any{"secret": true}, err: &response.Error{Code: 400, Payload: body}}
	invoker := New(Config{Invoker: captured, Output: func(exec.ComponentTarget) OutputEncoder {
		return func(context.Context, any) ([]byte, error) {
			t.Fatal("success encoder called for explicit error")
			return nil, nil
		}
	}, ErrorOutput: func(actual exec.ComponentTarget) OutputEncoder {
		if actual != target {
			t.Fatal("target lost")
		}
		return func(ctx context.Context, value any) ([]byte, error) {
			if ctx == nil || value != body {
				t.Fatal("error body/context lost")
			}
			return []byte(`{"items":[]}`), nil
		}
	}})
	execution, protocolErr := invoker.Execute(context.Background(), Request{Target: target, Method: "tools/call", URI: "error"})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	result := execution.ToolResult()
	if result.IsError == nil || !*result.IsError || result.Content[0].(schema.TextContent).Text != `{"items":[]}` {
		t.Fatalf("result=%+v", result)
	}
	execution.encodeErrorOutput = func(context.Context, any) ([]byte, error) { return nil, errors.New("private encoder failure") }
	result = execution.ToolResult()
	object := testharness.StructuredObject(t, result.StructuredContent)
	if result.IsError == nil || !*result.IsError || object["status"] != 500 || object["message"] != "Internal Server Error" {
		t.Fatalf("result=%+v", result)
	}
}

func TestRawExplicitErrorBypassesErrorOutputEncoder(t *testing.T) {
	execution := &Execution{err: &response.Error{Code: 400, Payload: &singleBodyResponse{payload: []byte(`{"items":null}`), status: 400}}, encodeErrorOutput: func(context.Context, any) ([]byte, error) {
		t.Fatal("raw response passed to error encoder")
		return nil, nil
	}}
	result := execution.ToolResult()
	if result.IsError == nil || !*result.IsError || result.StructuredContent != nil {
		t.Fatalf("result=%+v", result)
	}
}
