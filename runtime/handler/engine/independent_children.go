package engine

import (
	"context"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
)

// validateIndependentChildren guards ownership before binding can execute any
// dependency. It never detaches context or hides an inherited transaction.
func validateIndependentChildren(ctx context.Context, request Request) error {
	reject := func(reason string) error { return &dexec.IndependentChildTransactionError{Reason: reason} }
	supported, ok := request.Handler.(rhandler.IndependentChildOrchestrator)
	if !ok || !supported.SupportsIndependentChildTransactions() {
		return reject("handler is not a custom orchestration contract")
	}
	if request.DataSource != nil {
		return reject("root has a data source")
	}
	if inherited, _ := ctx.Value(dataScopeContextKey{}).(*dataScope); inherited != nil {
		return reject("caller has an inherited managed unit")
	}
	if _, ok := request.Handler.(rhandler.OutcomeFinalizer); ok {
		return reject("root owns outcome finalization")
	}
	if request.hasInjectorFinalizer() {
		return reject("root owns injector finalization")
	}
	if request.Completion != nil {
		return reject("root owns completion observation")
	}
	if request.SequenceStrategy != "" {
		return reject("root declares a sequence strategy")
	}
	return nil
}
