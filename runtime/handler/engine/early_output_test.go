package engine

import (
	"context"
	"errors"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	xlogger "github.com/viant/xdatly/logger"
	"github.com/viant/xdatly/response"
)

type earlyOutputInput struct {
	Name string `parameter:"Name,kind=query,in=name,required,errorCode=418,errorMessage=missing name"`
}
type earlyOutputSink struct{ calls int }

func (*earlyOutputSink) Debug(string, ...any)  {}
func (s *earlyOutputSink) Info(string, ...any) { s.calls++ }
func (*earlyOutputSink) Warn(string, ...any)   {}
func (*earlyOutputSink) Error(string, ...any)  {}

type earlyOutput struct {
	Logger xlogger.Logger `bind:"kind=logger,required" json:"-"`
	calls  int
	cause  error
}

func (o *earlyOutput) Finalize(_ context.Context, cause error) error {
	o.calls++
	o.cause = cause
	if o.Logger != nil {
		o.Logger.Info("finalized")
	}
	if cause != nil {
		return &response.Error{Code: 409, Payload: map[string]any{"selected": "application", "result": nil}, Cause: cause}
	}
	return nil
}

type earlyOutputHandler struct {
	calls int
	fail  error
}

func (h *earlyOutputHandler) Execute(context.Context, handler.Invocation) (any, error) {
	h.calls++
	if h.fail != nil {
		return nil, h.fail
	}
	return &earlyOutput{}, nil
}

func TestEarlyOutputFinalizerUsesStaticCapabilitiesAndPreservesBindingCause(t *testing.T) {
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.test/callback", Name: "Names"}, Routes: []*spec.Route{{Method: "GET", Path: "/names"}}}
	bindings, err := (compiler.OutputBindingCompiler{Component: component, Type: reflect.TypeFor[earlyOutput](), Kinds: []string{"logger"}}).CompileBindings()
	if err != nil {
		t.Fatal(err)
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(reflect.TypeFor[earlyOutput](), bindings...)
	if err != nil {
		t.Fatal(err)
	}
	sink := &earlyOutputSink{}
	h := &earlyOutputHandler{}
	result, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), OutputType: reflect.TypeFor[earlyOutput](), OutputCapabilities: plan, Handler: h, Capabilities: handler.InvocationCapabilities{Logger: sink}})
	if h.calls != 0 {
		t.Fatal("required input failure executed handler")
	}
	o, ok := result.(*earlyOutput)
	if !ok || o == nil {
		t.Fatalf("callback output type=%T", result)
	}
	if o.calls != 1 || o.Logger != sink || sink.calls != 1 {
		t.Fatalf("callback/static capability mismatch: result%T calls%d logger%T sink%d err%v", result, o.calls, o.Logger, sink.calls, err)
	}
	var bindErr *bindly.BindingError
	if !errors.As(err, &bindErr) || bindErr.Path != "Name" || bindErr.StatusCode() != 418 {
		t.Fatalf("binding metadata/cause lost:%v", err)
	}
	if response.ErrorStatusCode(err, 500) != 409 {
		t.Fatalf("application projection did not select status:%v", err)
	}
	body, explicit := response.ErrorBody(err)
	if !explicit || !reflect.DeepEqual(body, map[string]any{"selected": "application", "result": nil}) {
		t.Fatal("explicit body changed")
	}
}

func earlyOutputPlan(t *testing.T) *bindly.Plan {
	t.Helper()
	bindings, err := (compiler.OutputBindingCompiler{Component: &spec.Component{}, Type: reflect.TypeFor[earlyOutput](), Kinds: []string{"logger"}}).CompileBindings()
	if err != nil {
		t.Fatal(err)
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(reflect.TypeFor[earlyOutput](), bindings...)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}
func TestOutputFinalizerOnceAcrossSuccessfulAndFailedExecution(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
	}{{"success", nil}, {"execution failure", errors.New("read execution fault")}} {
		t.Run(tc.name, func(t *testing.T) {
			request := testharness.NewRequest("GET", "/names").WithQuery(url.Values{"name": {"provided"}})
			scope, err := request.Scope()
			if err != nil {
				t.Fatal(err)
			}
			defer scope.Close()
			sink := &earlyOutputSink{}
			h := &earlyOutputHandler{fail: tc.failure}
			value, err := New().Execute(context.Background(), Request{Scope: scope, Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), OutputType: reflect.TypeFor[earlyOutput](), OutputCapabilities: earlyOutputPlan(t), Handler: h, Capabilities: handler.InvocationCapabilities{Logger: sink}})
			o, ok := value.(*earlyOutput)
			if !ok || o == nil {
				t.Fatalf("output%T err%v", value, err)
			}
			if h.calls != 1 || o.calls != 1 || sink.calls != 1 || o.Logger != sink {
				t.Fatalf("execution/finalizer count: %d/%d/%d", h.calls, o.calls, sink.calls)
			}
			if tc.failure == nil && err != nil {
				t.Fatal(err)
			}
			if tc.failure != nil && !errors.Is(err, tc.failure) {
				t.Fatal("execution cause lost")
			}
		})
	}
}
func TestMissingStaticOutputCapabilityPreservesOriginalFailure(t *testing.T) {
	h := &earlyOutputHandler{}
	value, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), OutputType: reflect.TypeFor[earlyOutput](), OutputCapabilities: earlyOutputPlan(t), Handler: h})
	o, ok := value.(*earlyOutput)
	if !ok || o == nil {
		t.Fatalf("output%T", value)
	}
	if o.Logger != nil || o.calls != 1 || h.calls != 0 {
		t.Fatal("missing static capability acquired a fallback")
	}
	var binding *bindly.BindingError
	if !errors.As(err, &binding) || binding.Path != "Name" {
		t.Fatalf("original binding failure lost:%v", err)
	}
	// The application sees both causes and chooses its own projection. The
	// engine does not turn a missing configured service into a global lookup.
	if o.cause == nil {
		t.Fatal("missing combined cause")
	}
}
func TestSuccessOnlyOutputIsNotCreatedOrFinalizedOnEarlyFailure(t *testing.T) {
	h := &earlyOutputHandler{}
	value, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), OutputType: reflect.TypeFor[finalizingOutput](), Handler: h})
	if err == nil || value != nil || h.calls != 0 {
		t.Fatal("success-only output ran on early failure")
	}
}

type panicErrorOutput struct{ calls int }

func (o *panicErrorOutput) Finalize(context.Context, error) error {
	o.calls++
	panic("private callback fault")
}

type completedEarlyData struct {
	preBindingData
	begins, completes, rollbacks int
	cause                        error
}

func (d *completedEarlyData) BeginInvocation() error { d.begins++; return nil }
func (d *completedEarlyData) Complete(_ context.Context, cause error) error {
	d.completes++
	d.cause = cause
	if cause != nil {
		d.rollbacks++
	}
	return cause
}

type panicEarlyHandler struct{ calls int }

func (*panicEarlyHandler) RequiresPreBindingTransaction() bool { return true }
func (h *panicEarlyHandler) Execute(context.Context, handler.Invocation) (any, error) {
	h.calls++
	return nil, nil
}

func TestEarlyErrorFinalizerPanicStillCompletesManagedScopeOnce(t *testing.T) {
	data := &completedEarlyData{}
	source := &staticDataSource{data: data}
	h := &panicEarlyHandler{}
	value, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), OutputType: reflect.TypeFor[panicErrorOutput](), DataSource: source, Handler: h})
	o, ok := value.(*panicErrorOutput)
	if !ok || o == nil || o.calls != 1 {
		t.Fatalf("callback output %T", value)
	}
	var binding *bindly.BindingError
	var panicErr *dexec.PanicError
	if !errors.As(err, &binding) || binding.Path != "Name" || !errors.As(err, &panicErr) {
		t.Fatalf("original/panic causes lost:%v", err)
	}
	if !data.started || data.begins != 1 || data.completes != 1 || data.rollbacks != 1 || h.calls != 0 {
		t.Fatalf("managed completion changed: started%v begin%d complete%d rollback%d handler%d", data.started, data.begins, data.completes, data.rollbacks, h.calls)
	}
	if data.cause == nil {
		t.Fatal("completion did not observe callback panic")
	}
}
func TestExecutionErrorFinalizerPanicDoesNotInvokeCallbackTwice(t *testing.T) {
	output := &panicErrorOutput{}
	value, err := New().Execute(context.Background(), Request{Input: testRouteInput(t, reflect.TypeFor[struct{}]()), OutputType: reflect.TypeFor[panicErrorOutput](), Handler: handler.HandlerFunc(func(context.Context, handler.Invocation) (any, error) { return output, nil })})
	var panicErr *dexec.PanicError
	if value != output || output.calls != 1 || !errors.As(err, &panicErr) {
		t.Fatalf("callback count/output/error changed:%d %T %v", output.calls, value, err)
	}
}
