package custom

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
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

type staticInput struct{}
type staticOutput struct{}

type staticContract struct {
	Dependency string `bind:"kind=static,required"`
}

type requestBoundContract struct {
	Token string `bind:"kind=header,in=Authorization,required"`
}

func (requestBoundContract) Exec(context.Context, xhandler.Session, *staticInput, *staticOutput) error {
	return nil
}

func (s staticContract) Exec(context.Context, xhandler.Session, *staticInput, *staticOutput) error {
	return nil
}

func TestStaticHandlerBindingsRequirePointerAndBindOnce(t *testing.T) {
	first, err := bindly.NewInjector(bindly.WithProviders(handlerprovider.Static(xhandler.ValueKey("static"), "first")))
	if err != nil {
		t.Fatal(err)
	}
	value := New[staticInput, staticOutput](staticContract{})
	if err := value.(interface {
		BindStatic(context.Context, *bindly.Injector) error
	}).BindStatic(context.Background(), first); err == nil || !strings.Contains(err.Error(), "pointer contract") {
		t.Fatalf("value contract binding error = %v", err)
	}
	contract := &staticContract{}
	bound := New[staticInput, staticOutput](contract)
	binder := bound.(interface {
		BindStatic(context.Context, *bindly.Injector) error
	})
	if err := binder.BindStatic(context.Background(), first); err != nil || contract.Dependency != "first" {
		t.Fatalf("first static binding = %q, %v", contract.Dependency, err)
	}
	second, err := bindly.NewInjector(bindly.WithProviders(handlerprovider.Static(xhandler.ValueKey("static"), "second")))
	if err != nil {
		t.Fatal(err)
	}
	if err := binder.BindStatic(context.Background(), second); err != nil || contract.Dependency != "first" {
		t.Fatalf("one-time static binding changed = %q, %v", contract.Dependency, err)
	}
}

func TestHandlerStaticBindingRejectsRequestScopeEvenWithProvider(t *testing.T) {
	injector, err := bindly.NewInjector(bindly.WithProviders(handlerprovider.Static(xhandler.ValueKey("header"), "Bearer forged")))
	if err != nil {
		t.Fatal(err)
	}
	contract := &requestBoundContract{}
	handler := New[staticInput, staticOutput](contract)
	err = handler.(interface {
		BindStatic(context.Context, *bindly.Injector) error
	}).BindStatic(context.Background(), injector)
	if err == nil || !strings.Contains(err.Error(), "request-scoped") || contract.Token != "" {
		t.Fatalf("request-scoped handler binding = %q, %v", contract.Token, err)
	}
}
