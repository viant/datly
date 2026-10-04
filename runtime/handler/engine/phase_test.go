package engine

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
	h "github.com/viant/xdatly/handler"
	xlogger "github.com/viant/xdatly/logger"
	"reflect"
	"testing"
)

type phaseEngineProbe struct {
	cancel                      context.CancelFunc
	terminalCanceled            bool
	events                      []h.PhaseEvent
	calls                       int
	fail                        error
	panicHandler, panicObserver bool
	logger                      xlogger.Logger
}

func (p *phaseEngineProbe) NewPhaseObserver() h.PhaseObserver { return p }
func (p *phaseEngineProbe) ObservePhase(ctx context.Context, e h.PhaseEvent) {
	if p.panicObserver {
		panic("observer")
	}
	if xlogger.FromContext(ctx) != p.logger {
		panic("wrong logger")
	}
	p.events = append(p.events, e)
	if e.Boundary == h.PhaseEnd {
		p.terminalCanceled = ctx.Err() != nil
	}
}
func (p *phaseEngineProbe) Execute(context.Context, rhandler.Invocation) (any, error) {
	p.calls++
	if p.cancel != nil {
		p.cancel()
		return nil, context.Canceled
	}
	if p.panicHandler {
		panic("handler panic")
	}
	return "result", p.fail
}
func TestEnginePhaseObservationPreservesBindingAndHandlerResults(t *testing.T) {
	for _, name := range []string{"success", "binding failure", "handler failure", "handler panic", "observer panic"} {
		t.Run(name, func(t *testing.T) {
			sink := &earlyOutputSink{}
			probe := &phaseEngineProbe{logger: sink}
			request := Request{Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), Handler: probe, BoundInput: &earlyOutputInput{Name: "bound"}, Capabilities: rhandler.InvocationCapabilities{Logger: sink}}
			if name == "binding failure" {
				request.BoundInput = nil
			}
			if name == "handler failure" {
				probe.fail = errors.New("operation")
			}
			probe.panicHandler = name == "handler panic"
			probe.panicObserver = name == "observer panic"
			result, err := New().Execute(context.Background(), request)
			if name == "observer panic" {
				if result != "result" || err != nil {
					t.Fatal("observer changed result")
				}
				return
			}
			if len(probe.events) < 4 {
				t.Fatalf("missing real boundaries: %+v", probe.events)
			}
			first, last := probe.events[0], probe.events[len(probe.events)-1]
			if first.Phase != h.PhaseInvocation || first.Boundary != h.PhaseBegin || last.Phase != h.PhaseInvocation || last.Boundary != h.PhaseEnd || first.InvocationID != last.InvocationID {
				t.Fatal("invocation boundaries changed")
			}
			switch name {
			case "success":
				if result != "result" || err != nil || last.Result != h.PhaseSucceeded {
					t.Fatal("successful outcome changed")
				}
			case "binding failure":
				var binding *bindly.BindingError
				if !errors.As(err, &binding) || probe.calls != 0 || last.Result != h.PhaseFailed {
					t.Fatal("binding failure changed")
				}
				for _, event := range probe.events {
					if event.Phase == h.PhaseInputInitialization {
						t.Fatal("skipped initialization emitted")
					}
				}
			case "handler failure":
				if !errors.Is(err, probe.fail) || last.Result != h.PhaseFailed {
					t.Fatal("cause changed")
				}
			case "handler panic":
				if err == nil || last.Result != h.PhasePanicked {
					t.Fatal("panic terminal notification missing")
				}
			}
		})
	}
}

type factoryPanicProbe struct{ calls int }

func (*factoryPanicProbe) NewPhaseObserver() h.PhaseObserver { panic("private factory detail") }
func (p *factoryPanicProbe) Execute(context.Context, rhandler.Invocation) (any, error) {
	p.calls++
	return "unchanged", nil
}

type factoryDiagnosticSink struct {
	messages    []string
	panicLogger bool
}

func (*factoryDiagnosticSink) Debug(string, ...any) {}
func (*factoryDiagnosticSink) Info(string, ...any)  {}
func (*factoryDiagnosticSink) Warn(string, ...any)  {}
func (s *factoryDiagnosticSink) Error(message string, args ...any) {
	s.messages = append(s.messages, message)
	if len(args) != 0 {
		panic("unexpected private details")
	}
	if s.panicLogger {
		panic("logger failure")
	}
}
func TestEnginePhaseFactoryPanicIsDiagnosticOnly(t *testing.T) {
	for _, loggerPanics := range []bool{false, true} {
		t.Run(fmt.Sprint(loggerPanics), func(t *testing.T) {
			sink := &factoryDiagnosticSink{panicLogger: loggerPanics}
			probe := &factoryPanicProbe{}
			result, err := New().Execute(context.Background(), Request{
				Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), Handler: probe,
				BoundInput:   &earlyOutputInput{Name: "bound"},
				Capabilities: rhandler.InvocationCapabilities{Logger: sink},
			})
			if err != nil || result != "unchanged" || probe.calls != 1 {
				t.Fatalf("factory panic changed execution: %v %v", result, err)
			}
			if !reflect.DeepEqual(sink.messages, []string{"phase observer factory panicked"}) {
				t.Fatalf("unexpected diagnostics: %v", sink.messages)
			}
		})
	}
}

func TestEnginePhaseCancellationRetainsCauseAndDeliversTerminalEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink := &earlyOutputSink{}
	probe := &phaseEngineProbe{logger: sink, cancel: cancel}
	_, err := New().Execute(ctx, Request{
		Input: testRouteInput(t, reflect.TypeFor[earlyOutputInput]()), Handler: probe,
		BoundInput: &earlyOutputInput{Name: "bound"}, Capabilities: rhandler.InvocationCapabilities{Logger: sink},
	})
	if !errors.Is(err, context.Canceled) || probe.calls != 1 || len(probe.events) < 4 {
		t.Fatalf("cancellation changed: %v", err)
	}
	last := probe.events[len(probe.events)-1]
	if last.Phase != h.PhaseInvocation || last.Boundary != h.PhaseEnd || last.Result != h.PhaseCanceled || !errors.Is(last.Cause, context.Canceled) || probe.terminalCanceled {
		t.Fatalf("terminal cancellation evidence missing: %+v", last)
	}
}
