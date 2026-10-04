package handler

import (
	"context"
	"errors"
	h "github.com/viant/xdatly/handler"
	xlogger "github.com/viant/xdatly/logger"
	"testing"
)

type phaseProbe struct {
	events           []h.PhaseEvent
	terminalCanceled bool
	panicObserver    bool
}

func (p *phaseProbe) ObservePhase(ctx context.Context, e h.PhaseEvent) {
	if p.panicObserver {
		panic("observer private detail")
	}
	p.events = append(p.events, e)
	if e.Boundary == h.PhaseEnd {
		p.terminalCanceled = ctx.Err() != nil
	}
}

type phaseLog struct{ errors int }

func (*phaseLog) Debug(string, ...any)   {}
func (*phaseLog) Info(string, ...any)    {}
func (*phaseLog) Warn(string, ...any)    {}
func (p *phaseLog) Error(string, ...any) { p.errors++ }
func TestPhaseScopePreservesOperationAndClassifiesTermination(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cause  error
		result h.PhaseResult
	}{
		{"success", nil, h.PhaseSucceeded}, {"failure", errors.New("operation failed"), h.PhaseFailed}, {"violations", ValidationPhaseFailure(&h.Validation{Failed: true}), h.PhaseViolations},
		{"operational validation error", &h.Validation{Failed: true}, h.PhaseFailed}, {"canceled", context.Canceled, h.PhaseCanceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &phaseProbe{}
			ctx, cancel := context.WithCancel(context.Background())
			ctx, scope := WithPhaseObserver(ctx, probe, 12, 2)
			err := scope.Run(ctx, h.PhaseValidation, func() error { cancel(); return tc.cause })
			if err != tc.cause || len(probe.events) != 2 || probe.events[0].Boundary != h.PhaseBegin || probe.events[1].Result != tc.result || probe.terminalCanceled {
				t.Fatal("phase result or terminal delivery changed")
			}
			for _, e := range probe.events {
				if e.InvocationID != 12 || e.Attempt != 2 {
					t.Fatal("attempt identity changed")
				}
			}
		})
	}
}
func TestPhaseScopeContainsObserverPanicsAndBoundsDiagnostics(t *testing.T) {
	log := &phaseLog{}
	ctx := xlogger.WithContext(context.Background(), log)
	ctx, scope := WithPhaseObserver(ctx, &phaseProbe{panicObserver: true}, 0, 0)
	failure := errors.New("original cause")
	for i := 0; i < 3; i++ {
		if err := scope.Run(ctx, h.PhaseBinding, func() error { return failure }); err != failure {
			t.Fatal("observer changed result")
		}
	}
	if log.errors != 1 {
		t.Fatalf("diagnostic count %d", log.errors)
	}
}
func TestPhaseScopePreservesOperationPanic(t *testing.T) {
	probe := &phaseProbe{}
	ctx, scope := WithPhaseObserver(context.Background(), probe, 0, 0)
	var caught any
	func() {
		defer func() { caught = recover() }()
		_ = scope.Run(ctx, h.PhaseQueue, func() error { panic("original panic") })
	}()
	if caught != "original panic" || len(probe.events) != 2 || probe.events[1].Result != h.PhasePanicked {
		t.Fatal("operation panic lost")
	}
}
func TestPhaseScopeOptOutAndIsolation(t *testing.T) {
	ctx := context.Background()
	same, scope := WithPhaseObserver(ctx, nil, 0, 0)
	if same != ctx || scope != nil {
		t.Fatal("opt-out changed context")
	}
	calls := 0
	if err := scope.Run(ctx, h.PhaseQueue, func() error { calls++; return nil }); err != nil || calls != 1 {
		t.Fatal("opt-out changed operation")
	}
	_, first := WithPhaseObserver(ctx, &phaseProbe{}, 0, 0)
	_, second := WithPhaseObserver(ctx, &phaseProbe{}, 0, 0)
	if first.InvocationID() == second.InvocationID() {
		t.Fatal("independent invocation identities collided")
	}
}

func TestPhaseScopeConcurrentInvocationsAndNestedOptOut(t *testing.T) {
	const workers = 32
	results := make(chan []h.PhaseEvent, workers)
	for i := 0; i < workers; i++ {
		go func() {
			probe := &phaseProbe{}
			ctx, scope := WithPhaseObserver(context.Background(), probe, 0, 0)
			_ = scope.Run(ctx, h.PhaseInvocation, func() error {
				child, childScope := WithPhaseObserver(ctx, nil, 0, 0)
				if childScope != nil || PhaseScopeFromContext(child) != nil {
					results <- nil
					return nil
				}
				_ = childScope.Run(child, h.PhaseQueue, func() error { return nil })
				return nil
			})
			results <- probe.events
		}()
	}
	seen := map[uint64]bool{}
	for i := 0; i < workers; i++ {
		events := <-results
		if len(events) != 2 {
			t.Fatalf("nested opt-out leaked events: %+v", events)
		}
		id := events[0].InvocationID
		if id == 0 || seen[id] {
			t.Fatalf("invocation identity reused: %d", id)
		}
		seen[id] = true
		if events[1].InvocationID != id || events[1].Result != h.PhaseSucceeded {
			t.Fatal("terminal event crossed invocation boundary")
		}
	}
}
