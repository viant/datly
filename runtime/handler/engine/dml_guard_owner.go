package engine

import (
	"context"

	"github.com/viant/datly/internal/drainowner"
	xhandler "github.com/viant/xdatly/handler"
)

// ValidateBoundDML verifies the already-resolved focused capability against its
// exact current scope and registered native journal. It never resolves data,
// invokes application callbacks, unwraps arbitrary providers or returns a native owner.
func ValidateBoundDML(ctx context.Context, service xhandler.DML, binding *drainowner.GuardBinding) error {
	if ctx == nil {
		return drainowner.ErrGuardBinding
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	scope, _ := ctx.Value(dataScopeContextKey{}).(*dataScope)
	if scope == nil {
		return drainowner.ErrGuardBinding
	}
	var capability dmlCapability
	var queue any
	switch value := service.(type) {
	case dmlCapability:
		capability = value
	case queueContractDMLCapability:
		capability = value.dmlCapability
		queue = value.queue
		if queue == nil {
			return drainowner.ErrOwner
		}
	default:
		return drainowner.ErrOwner
	}
	root := scope
	if scope.root != nil {
		root = scope.root
	}
	root.mu.Lock()
	closed, failure, expected := root.completionStarted, root.guardedFailure, scope.associationData
	root.mu.Unlock()
	if closed || failure != nil || expected == nil || capability.guard != scope.mutationGuard() {
		return drainowner.ErrGuardBinding
	}
	if err := capability.guard.check("source phase owner association"); err != nil {
		return err
	}
	return drainowner.ValidateBoundView(capability.service, expected, queue, binding)
}
