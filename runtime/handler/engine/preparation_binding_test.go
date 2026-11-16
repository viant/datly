package engine

import (
	"context"
	"errors"
	"github.com/viant/bindly"
	"github.com/viant/datly/internal/selectorcompanion"
	rhandler "github.com/viant/datly/runtime/handler"
	xhandler "github.com/viant/xdatly/handler"
	"testing"
)

type preparationProbe struct{ calls int }

func (p *preparationProbe) Execute(context.Context, rhandler.Invocation) (any, error) {
	p.calls++
	return struct{}{}, nil
}
func TestPreparationBindingRejectsForeignAndExecutionAuthority(t *testing.T) {
	route := companionRoute(t)
	for _, name := range []string{"nil", "zero", "copy", "handler", "contract", "bound", "resolved", "replay", "capture", "completion", "independent", "datasource"} {
		t.Run(name, func(t *testing.T) {
			h := &preparationProbe{}
			reader := new(int)
			proof := selectorcompanion.NewPreparationBinding(h, reader, route)
			request := Request{Input: route, Handler: h, PreparationBinding: proof}
			switch name {
			case "nil":
				request.PreparationBinding = nil
			case "zero":
				request.PreparationBinding = new(selectorcompanion.PreparationBinding)
			case "copy":
				copyProof := *proof
				request.PreparationBinding = &copyProof
			case "handler":
				request.Handler = &preparationProbe{}
			case "contract":
				request.Input = companionRoute(t)
			case "bound":
				request.BoundInput = new(struct{})
			case "resolved":
				request.ResolvedInput = new(struct{})
			case "replay":
				request.Replay = &bindly.ReplayBinding{}
			case "capture":
				request.BindingOutput = new(int)
			case "completion":
				request.Completion = func(xhandler.Outcome) {}
			case "independent":
				request.IndependentChildTransactions = true
			case "datasource":
				request.DataSource = &companionSource{}
			}
			if _, err := New().Execute(t.Context(), request); err == nil {
				t.Fatal("invalid proof admitted")
			}
			if h.calls != 0 {
				t.Fatal("rejection reached handler")
			}
			if request.CanonicalReader {
				t.Fatal("proof granted reader execution")
			}
		})
	}
}

func TestPreparationBindingDoesNotAdmitReadOnlyDependency(t *testing.T) {
	route := companionRoute(t)
	h := &preparationProbe{}
	proof := selectorcompanion.NewPreparationBinding(h, new(int), route)
	permit := &readOnlyDependency{group: &readOnlyGroup{guard: &mutationGuard{}}}
	ctx := context.WithValue(t.Context(), readOnlyDependencyKey{}, permit)
	_, err := New().Execute(ctx, Request{Input: route, Handler: h, PreparationBinding: proof})
	if !errors.Is(err, ErrReadOnlyDependency) {
		t.Fatalf("preparation acquired read-only reader authority: %v", err)
	}
	if h.calls != 0 {
		t.Fatal("read-only denial reached preparation")
	}
}
