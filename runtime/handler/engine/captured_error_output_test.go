package engine

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"

	dexec "github.com/viant/datly/exec"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/compiler"
	hp "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	xh "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
	xlogger "github.com/viant/xdatly/logger"
)

type capturedBridgeOutput struct {
	Value  int
	Logger xlogger.Logger `bind:"kind=logger,required" json:"-"`
	Probe  *int           `bind:"kind=capture_probe,required,cacheable=false" json:"-"`
}
type capturedBridgeProbe struct {
	outputType reflect.Type
	result     any
	failure    error
	panicValue any
	calls      int
	seenCause  error
}

func (p *capturedBridgeProbe) Execute(context.Context, rh.Invocation) (any, error) {
	panic("must not execute")
}
func (*capturedBridgeProbe) InputType() reflect.Type       { return reflect.TypeFor[struct{}]() }
func (*capturedBridgeProbe) EarlyErrorOutputEnabled() bool { return true }
func (p *capturedBridgeProbe) OutputType() reflect.Type    { return p.outputType }
func (p *capturedBridgeProbe) CapturedErrorOutput(_ context.Context, _ rh.Invocation, cause error) (any, error) {
	p.calls++
	p.seenCause = cause
	if p.panicValue != nil {
		panic(p.panicValue)
	}
	return p.result, p.failure
}

// This facade intentionally lacks CapturedErrorOutput.
type noCapturedBridge struct{ probe *capturedGateProbe }

func (h *noCapturedBridge) Execute(ctx context.Context, inv rh.Invocation) (any, error) {
	return h.probe.Execute(ctx, inv)
}
func (h *noCapturedBridge) CaptureInput(ctx context.Context, input any) (any, error) {
	return h.probe.CaptureInput(ctx, input)
}
func (h *noCapturedBridge) EarlyErrorOutputEnabled() bool { return h.probe.EarlyErrorOutputEnabled() }
func (h *noCapturedBridge) FinalizeOutcome(ctx context.Context, inv rh.Invocation, result any, outcome xh.Outcome) error {
	return h.probe.FinalizeOutcome(ctx, inv, result, outcome)
}
func (h *noCapturedBridge) InputType() reflect.Type  { return h.probe.InputType() }
func (h *noCapturedBridge) OutputType() reflect.Type { return h.probe.OutputType() }

func TestCapturedInitializationDeclinesWithoutNewCallbacksOrErrorWrapper(t *testing.T) {
	for _, mode := range []string{"absent bridge", "disabled", "missing declaration", "declined"} {
		t.Run(mode, func(t *testing.T) {
			p := &capturedGateProbe{capturedBridgeProbe: capturedBridgeProbe{outputType: reflect.TypeFor[capturedBridgeOutput]()}, early: true, optinPanic: mode == "absent bridge"}
			var h rh.Handler = p
			if mode == "absent bridge" {
				h = &noCapturedBridge{probe: p}
			}
			if mode == "disabled" {
				p.early = false
				p.result = &capturedBridgeOutput{}
			}
			declared := p.outputType
			if mode == "missing declaration" {
				declared = nil
			}
			value, err := New().Execute(t.Context(), Request{Input: testRouteInput(t, reflect.TypeFor[capturedGateInput]()), BoundInput: &capturedGateInput{Mode: "returned"}, Handler: h, OutputType: declared, OutputCapabilities: capturedCapabilityPlan(t)})
			if err == nil || value != nil || p.executeCalls != 0 || p.finalizeCalls != 1 {
				t.Fatalf("value=%T err=%v execute=%d final=%d", value, err, p.executeCalls, p.finalizeCalls)
			}
			if _, joined := err.(interface{ Unwrap() []error }); joined {
				t.Fatalf("decline introduced error wrapper: %T", err)
			}
			if mode == "absent bridge" || mode == "missing declaration" {
				if p.optinCalls != 0 || p.calls != 0 {
					t.Fatalf("inadmissible callback optin=%d bridge=%d", p.optinCalls, p.calls)
				}
			}
			if mode == "disabled" && p.calls != 0 {
				t.Fatal("disabled bridge invoked")
			}
			if mode == "declined" && (p.calls != 1 || err != p.seenCause) {
				t.Fatal("decline altered operation error identity")
			}
		})
	}
}

func TestCapturedInitializationOutputAdmission(t *testing.T) {
	typ := reflect.TypeFor[capturedBridgeOutput]()
	supplemental := errors.New("bridge failure")
	canonical := &capturedBridgeOutput{Value: 7}
	for _, tc := range []struct {
		name        string
		declared    reflect.Type
		result      any
		failure     error
		wantResult  any
		wantFailure bool
		wantCalls   int
	}{
		{name: "canonical", declared: typ, result: canonical, wantResult: canonical, wantCalls: 1},
		{name: "declined", declared: typ, wantCalls: 1},
		{name: "no declaration", result: canonical},
		{name: "wrong contract", declared: reflect.TypeFor[string](), wantFailure: true},
		{name: "typed nil", declared: typ, result: (*capturedBridgeOutput)(nil), wantFailure: true, wantCalls: 1},
		{name: "value instead of pointer", declared: typ, result: capturedBridgeOutput{}, wantFailure: true, wantCalls: 1},
		{name: "wrong result plus failure", declared: typ, result: &struct{}{}, failure: supplemental, wantFailure: true, wantCalls: 1},
		{name: "canonical plus failure", declared: typ, result: canonical, failure: supplemental, wantResult: canonical, wantFailure: true, wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &capturedBridgeProbe{outputType: typ, result: tc.result, failure: tc.failure}
			result, err := capturedInitializationOutput(t.Context(), Request{Handler: p, OutputType: tc.declared}, rh.Invocation{}, errors.New("original"))
			if result != tc.wantResult || (err != nil) != tc.wantFailure || p.calls != tc.wantCalls {
				t.Fatalf("result=%T error=%v calls=%d", result, err, p.calls)
			}
			if tc.failure != nil && !errors.Is(err, tc.failure) {
				t.Fatalf("additional bridge cause lost: %v", err)
			}
		})
	}
}

func TestCapturedInitializationOutputRecoversBridgePanic(t *testing.T) {
	cause := errors.New("bridge panic")
	p := &capturedBridgeProbe{outputType: reflect.TypeFor[capturedBridgeOutput](), panicValue: cause}
	result, err := capturedInitializationOutput(t.Context(), Request{Handler: p, OutputType: p.outputType}, rh.Invocation{}, errors.New("original"))
	var panicError *dexec.PanicError
	if result != nil || !errors.As(err, &panicError) || panicError.Cause() != cause || p.calls != 1 {
		t.Fatalf("result=%T err=%v calls=%d", result, err, p.calls)
	}
}

var capturedInitCause = errors.New("initialization business error")

type capturedGateInput struct {
	Mode  string
	Order []string
}

func (i *capturedGateInput) Init(ctx context.Context) error {
	i.Order = append(i.Order, "init")
	switch i.Mode {
	case "cancelled":
		return ctx.Err()
	case "returned":
		return capturedInitCause
	case "panic":
		panic(errors.New("initializer panic"))
	}
	return nil
}

func (i *capturedGateInput) InitMCP(context.Context, xmcp.Context) error {
	i.Order = append(i.Order, "mcp")
	switch i.Mode {
	case "returned-mcp":
		return errors.New("MCP business error")
	case "panic-mcp":
		panic(errors.New("MCP initializer panic"))
	}
	return nil
}

type capturedGateProbe struct {
	capturedBridgeProbe
	captureError                error
	executeError                error
	early                       bool
	optinCalls                  int
	optinPanic                  bool
	executeCalls, finalizeCalls int
	finalizedResult             any
}

type capturedCompletionProbe struct {
	capturedGateProbe
	data *completedEarlyData
	t    *testing.T
}

func (*capturedCompletionProbe) RequiresPreBindingTransaction() bool { return true }
func (p *capturedCompletionProbe) FinalizeOutcome(ctx context.Context, inv rh.Invocation, result any, outcome xh.Outcome) error {
	if p.data.completes != 1 {
		p.t.Fatalf("finalization before cleanup: %d", p.data.completes)
	}
	return p.capturedGateProbe.FinalizeOutcome(ctx, inv, result, outcome)
}

func capturedCapabilityPlan(t *testing.T) *bindly.Plan {
	t.Helper()
	bindings, err := (compiler.OutputBindingCompiler{Component: &spec.Component{}, Type: reflect.TypeFor[capturedBridgeOutput](), Kinds: []string{"logger", "capture_probe"}}).CompileBindings()
	if err != nil {
		t.Fatal(err)
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	plan, err := injector.CompilePlan(reflect.TypeFor[capturedBridgeOutput](), bindings...)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestCapturedInitializationOutputCapabilitiesAndCleanup(t *testing.T) {
	for _, mode := range []string{"success", "missing capability", "bridge panic", "provider error", "provider panic", "optin panic"} {
		t.Run(mode, func(t *testing.T) {
			data := &completedEarlyData{}
			canonical := &capturedBridgeOutput{Value: 9}
			p := &capturedCompletionProbe{capturedGateProbe: capturedGateProbe{capturedBridgeProbe: capturedBridgeProbe{outputType: reflect.TypeFor[capturedBridgeOutput](), result: canonical}, early: true}, data: data, t: t}
			sink := &earlyOutputSink{}
			caps := rh.InvocationCapabilities{Logger: sink}
			providerCalls := 0
			providerCause := errors.New("output provider failure")
			provider := hp.Named("capture_probe", func(context.Context, reflect.Type, string) (any, bool, error) {
				providerCalls++
				if mode == "provider panic" {
					panic(providerCause)
				}
				if mode == "provider error" {
					return nil, false, providerCause
				}
				n := 17
				return &n, true, nil
			})
			injector, buildErr := bindly.NewInjector(bindly.WithProviders(provider))
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			if mode == "missing capability" {
				caps.Logger = nil
			}
			if mode == "optin panic" {
				p.optinPanic = true
			}
			if mode == "bridge panic" {
				p.panicValue = errors.New("bridge panic")
			}
			result, err := New().Execute(t.Context(), Request{Injector: injector, Input: testRouteInput(t, reflect.TypeFor[capturedGateInput]()), BoundInput: &capturedGateInput{Mode: "returned"}, Handler: p, OutputType: p.outputType, OutputCapabilities: capturedCapabilityPlan(t), Capabilities: caps, DataSource: &staticDataSource{data: data}})
			if err == nil || p.executeCalls != 0 || (p.calls != 1 && mode != "optin panic" || p.calls != 0 && mode == "optin panic") || p.finalizeCalls != 1 || data.completes != 1 {
				t.Fatalf("err=%v bridge=%d exec=%d finalize=%d complete=%d", err, p.calls, p.executeCalls, p.finalizeCalls, data.completes)
			}
			if mode == "bridge panic" || mode == "optin panic" {
				var panicErr *dexec.PanicError
				if result != nil || !errors.As(err, &panicErr) {
					t.Fatalf("panic=%v result=%T", err, result)
				}
			} else {
				if result != canonical || p.finalizedResult != canonical {
					t.Fatal("canonical finalizer identity lost")
				}
				if mode == "success" && canonical.Logger != sink {
					t.Fatal("canonical capability not bound")
				}
				if mode == "success" && (providerCalls != 1 || canonical.Probe == nil || *canonical.Probe != 17) {
					t.Fatalf("capability binding count=%d probe=%v", providerCalls, canonical.Probe)
				}
				if mode == "provider error" && (!errors.Is(err, providerCause) || providerCalls != 1) {
					t.Fatalf("provider cause/count lost: %v/%d", err, providerCalls)
				}
				if mode == "provider panic" {
					var panicErr *dexec.PanicError
					if !errors.As(err, &panicErr) || panicErr.Cause() != providerCause || providerCalls != 1 {
						t.Fatalf("provider panic/count lost: %v/%d", err, providerCalls)
					}
				}
				if mode == "missing capability" {
					var bindingErr *bindly.BindingError
					if !errors.As(err, &bindingErr) {
						t.Fatalf("binding error lost: %v", err)
					}
				}
			}
			if data.rollbacks != 1 || !errors.Is(err, capturedInitCause) {
				t.Fatalf("rollback/cause identity lost: rollback=%d err=%v", data.rollbacks, err)
			}
			if !strings.Contains(err.Error(), "initialization business error") {
				t.Fatalf("original initialization error lost: %v", err)
			}
		})
	}
}

func TestCapturedInitializationCancellationCauseIsPreserved(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	p := &capturedGateProbe{capturedBridgeProbe: capturedBridgeProbe{outputType: reflect.TypeFor[capturedBridgeOutput](), result: nil}, early: true}
	result, err := New().Execute(ctx, Request{Input: testRouteInput(t, reflect.TypeFor[capturedGateInput]()), BoundInput: &capturedGateInput{Mode: "cancelled"}, Handler: p, OutputType: p.outputType})
	if !errors.Is(err, context.Canceled) || p.executeCalls != 0 || p.calls != 1 || result != nil {
		t.Fatalf("err=%v execute=%d bridge=%d result=%T", err, p.executeCalls, p.calls, result)
	}
}

func TestCapturedInitializationMCPBoundaries(t *testing.T) {
	for _, mode := range []string{"returned-mcp", "panic-mcp", "returned"} {
		t.Run(mode, func(t *testing.T) {
			input := &capturedGateInput{Mode: mode}
			canonical := &capturedBridgeOutput{}
			p := &capturedGateProbe{capturedBridgeProbe: capturedBridgeProbe{outputType: reflect.TypeFor[capturedBridgeOutput](), result: canonical}, early: true}
			value, err := New().Execute(xmcp.WithContext(t.Context(), engineMCPContext{}), Request{Input: testRouteInput(t, reflect.TypeFor[capturedGateInput]()), BoundInput: input, Handler: p, OutputType: p.outputType})
			if err == nil || p.executeCalls != 0 || p.finalizeCalls != 1 {
				t.Fatalf("err=%v exec=%d final=%d", err, p.executeCalls, p.finalizeCalls)
			}
			expected := []string{"init", "mcp"}
			if mode == "returned" {
				expected = []string{"init"}
			}
			if !reflect.DeepEqual(input.Order, expected) {
				t.Fatalf("order=%v", input.Order)
			}
			if mode == "panic-mcp" {
				if value != nil || p.calls != 0 {
					t.Fatal("panic used returned-error bridge")
				}
			} else if value != canonical || p.calls != 1 {
				t.Fatal("returned error lost canonical bridge")
			}
		})
	}
}

func (*capturedGateProbe) InputType() reflect.Type { return reflect.TypeFor[capturedGateInput]() }
func (p *capturedGateProbe) CaptureInput(context.Context, any) (any, error) {
	return &struct{}{}, p.captureError
}
func (p *capturedGateProbe) Execute(context.Context, rh.Invocation) (any, error) {
	p.executeCalls++
	return nil, p.executeError
}
func (p *capturedGateProbe) EarlyErrorOutputEnabled() bool {
	p.optinCalls++
	if p.optinPanic {
		panic(errors.New("optin panic"))
	}
	return p.early
}
func (p *capturedGateProbe) FinalizeOutcome(_ context.Context, _ rh.Invocation, result any, _ xh.Outcome) error {
	p.finalizeCalls++
	p.finalizedResult = result
	return nil
}

func TestCapturedInitializationBridgeLifecycleGates(t *testing.T) {
	for _, tc := range []struct {
		name, mode                            string
		captureFailure, executeFailure, early bool
		bridgeCalls, executeCalls             int
		typedResult                           bool
	}{
		{name: "returned initializer", mode: "returned", early: true, bridgeCalls: 1, typedResult: true},
		{name: "initializer panic", mode: "panic", early: true},
		{name: "partial capture", mode: "returned", captureFailure: true, early: true},
		{name: "execute failure", executeFailure: true, early: true, executeCalls: 1},
		{name: "not opted in", mode: "returned"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			canonical := &capturedBridgeOutput{Value: 9}
			p := &capturedGateProbe{capturedBridgeProbe: capturedBridgeProbe{outputType: reflect.TypeFor[capturedBridgeOutput](), result: canonical}, early: tc.early}
			if tc.captureFailure {
				p.captureError = errors.New("partial capture")
			}
			if tc.executeFailure {
				p.executeError = errors.New("late execute")
			}
			input := testRouteInput(t, reflect.TypeFor[capturedGateInput]())
			result, err := New().Execute(t.Context(), Request{Input: input, BoundInput: &capturedGateInput{Mode: tc.mode}, Handler: p, OutputType: p.outputType})
			if err == nil || p.calls != tc.bridgeCalls || p.executeCalls != tc.executeCalls || p.finalizeCalls != 1 {
				t.Fatalf("err=%v bridge=%d execute=%d finalize=%d", err, p.calls, p.executeCalls, p.finalizeCalls)
			}
			expectedOptin := 0
			if tc.mode == "returned" && !tc.captureFailure {
				expectedOptin = 1
			}
			if p.optinCalls != expectedOptin {
				t.Fatalf("excluded path called optin %d times, expected%d", p.optinCalls, expectedOptin)
			}
			if tc.typedResult {
				if result != canonical || p.finalizedResult != canonical {
					t.Fatalf("canonical identity result=%T final=%T", result, p.finalizedResult)
				}
			} else if result != nil || p.finalizedResult != nil {
				t.Fatalf("excluded path manufactured result: result=%T final=%T", result, p.finalizedResult)
			}
		})
	}
}
