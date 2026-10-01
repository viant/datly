package runtime

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

type outputCapabilityInput struct {
	Name string `parameter:"Name,kind=query,in=name,required,errorCode=400,errorMessage=name required"`
}
type outputCapabilityLogger struct {
	mu    sync.Mutex
	calls int
}

func (*outputCapabilityLogger) Debug(string, ...any)  {}
func (l *outputCapabilityLogger) Info(string, ...any) { l.mu.Lock(); defer l.mu.Unlock(); l.calls++ }
func (*outputCapabilityLogger) Warn(string, ...any)   {}
func (*outputCapabilityLogger) Error(string, ...any)  {}

type outputCapabilityOutput struct {
	Logger   xhandler.Logger `parameter:"Logger,kind=logger,required" json:"-"`
	calls    int
	observed error
}

func (o *outputCapabilityOutput) Finalize(_ context.Context, failure error) error {
	o.calls++
	o.observed = failure
	if o.Logger == nil {
		return errors.New("configured logger not injected")
	}
	o.Logger.Info("completed")
	if failure != nil {
		return &response.Error{Code: 409, Payload: map[string]any{"result": nil, "reason": "selected callback"}, Cause: failure}
	}
	return nil
}

type outputCapabilityReader struct {
	calls   int
	failure error
}

func (r *outputCapabilityReader) Read(context.Context, any, xhandler.Binder, sqlx.ParameterResolver) (any, error) {
	r.calls++
	if r.failure != nil {
		return nil, r.failure
	}
	return &outputCapabilityOutput{}, nil
}

func TestRuntimeOutputCapabilityAndEarlyFinalizerRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name    string
		query   bool
		failure error
	}{{"missing required", false, nil}, {"successful read", true, nil}, {"read fault", true, errors.New("private query fault")}} {
		t.Run(tt.name, func(t *testing.T) {
			c := componentSpec("Values", http.MethodGet, "/values", nil)
			a := componentArtifact(t, c, reflect.TypeFor[outputCapabilityInput](), reflect.TypeFor[outputCapabilityOutput]())
			// Go discovery must classify the nonpublic output capability separately
			// from request fields, matching the source-preserving DQL output tag.
			found := false
			for _, p := range a.Component.Parameters {
				if p.Name == "Logger" {
					found = p.EmitOutput && p.Source.Kind == "logger"
				}
			}
			if !found {
				t.Fatal("output capability metadata missing")
			}
			sink := &outputCapabilityLogger{}
			reader := &outputCapabilityReader{failure: tt.failure}
			rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[outputCapabilityOutput](), Reader: reader, Capabilities: rhandler.InvocationCapabilities{Logger: sink}}})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(context.Background())
			request := testharness.NewRequest(http.MethodGet, "/values")
			if tt.query {
				request = request.WithQuery(url.Values{"name": {"provided"}, "Logger": {"forged"}})
			}
			value, err := executeTestRoute(t, rt, context.Background(), request)
			o, ok := value.(*outputCapabilityOutput)
			if !ok || o == nil {
				t.Fatalf("output %T err%v", value, err)
			}
			if o.Logger != sink || o.calls != 1 || sink.calls != 1 {
				t.Fatal("configured capability/finalizer count changed")
			}
			if !tt.query {
				var failure *bindly.BindingError
				if !errors.As(err, &failure) || failure.Path != "Name" || reader.calls != 0 {
					t.Fatal("early failure metadata or no-read gate changed")
				}
			} else if tt.failure != nil {
				if !errors.Is(err, tt.failure) || reader.calls != 1 {
					t.Fatal("read failure cause lost")
				}
			} else if err != nil || reader.calls != 1 {
				t.Fatalf("successful execution: %v", err)
			}
		})
	}
}

var _ = spec.KindComponent
