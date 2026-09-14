package custom

import (
	"context"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
)

type testBinder struct {
	input any
}

func (b *testBinder) Bind(context.Context, any) error {
	return nil
}

func (b *testBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	if key == xhandler.InputKey {
		return b.input, true, nil
	}
	return nil, false, nil
}

type testResponse struct {
	status int
}

func (r *testResponse) StatusCode() int           { return r.status }
func (r *testResponse) SetStatusCode(code int)    { r.status = code }
func (*testResponse) AddError(error)              {}
func (*testResponse) AddMetric(*xresponse.Metric) {}
func (*testResponse) Metrics() xresponse.Metrics  { return nil }

func TestContractHandlerUsesCanonicalInputSessionAndOutput(t *testing.T) {
	type input struct{ Name string }
	type output struct{ Name string }
	in := &input{Name: "Ada"}
	binder := &testBinder{input: in}
	response := &testResponse{}
	handler := New[input, output](xhandler.ContractFunc[input, output](func(ctx context.Context, sess xhandler.Session, actual *input, out *output) error {
		if actual != in {
			t.Fatalf("expected canonical input pointer")
		}
		if fromContext, ok := xhandler.SessionFromContext(ctx); !ok || fromContext != sess {
			t.Fatalf("expected custom session in invocation context")
		}
		bound, found, err := sess.Binder().Lookup(ctx, xhandler.InputKey)
		if err != nil || !found || bound != in {
			t.Fatalf("unexpected canonical input lookup: value=%#v found=%v err=%v", bound, found, err)
		}
		out.Name = actual.Name
		sess.Response().SetStatusCode(202)
		return nil
	}))

	actual, err := handler.Execute(context.Background(), rhandler.Invocation{
		Input:    in,
		Binder:   binder,
		Response: response,
	})
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if actual.(*output).Name != "Ada" || response.status != 202 {
		t.Fatalf("unexpected output/response: output=%#v status=%d", actual, response.status)
	}
	if handler.InputType() != reflect.TypeOf(input{}) || handler.OutputType() != reflect.TypeOf(output{}) {
		t.Fatalf("unexpected typed handler surface")
	}
}

func TestFuncHandler(t *testing.T) {
	type input struct{ Value int }
	type output struct{ Value int }
	handler := NewFunc[input, output](func(_ context.Context, in *input) (*output, error) {
		return &output{Value: in.Value + 1}, nil
	})
	actual, err := handler.Execute(context.Background(), rhandler.Invocation{Input: &input{Value: 4}})
	if err != nil || actual.(*output).Value != 5 {
		t.Fatalf("unexpected function result: actual=%#v err=%v", actual, err)
	}
}
