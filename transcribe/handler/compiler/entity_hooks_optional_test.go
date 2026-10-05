package compiler

import (
	"context"
	"reflect"
	"testing"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	h "github.com/viant/xdatly/handler"
)

type optionalEntityHooks struct{ rootEntityHooks }
type hookInput struct{}
type hookOutput struct{}

func (*optionalEntityHooks) Finalize(context.Context, *hookInput, *hookOutput, h.Outcome) error {
	return nil
}

type wrongCompletionHooks struct{ rootEntityHooks }

type recoveryEntityHooks struct{ rootEntityHooks }

func (*recoveryEntityHooks) Recover(context.Context, *hookInput, *hookOutput, rhandler.MutationOutcome) (rhandler.Recovery, error) {
	return rhandler.RecoveryNone, nil
}

type wrongRecoveryHooks struct{ rootEntityHooks }

type transactionRetryHooks struct{ rootEntityHooks }

func (*transactionRetryHooks) RetryTransaction(context.Context, *hookInput, *hookOutput, rhandler.MutationOutcome) (bool, error) {
	return false, nil
}

type wrongTransactionRetryHooks struct{ rootEntityHooks }

func (*wrongTransactionRetryHooks) RetryTransaction(context.Context, *hookInput, *hookOutput, rhandler.MutationOutcome) (rhandler.Recovery, error) {
	return rhandler.RecoveryNone, nil
}

func (*wrongRecoveryHooks) Recover(context.Context, *hookInput, *hookOutput, h.Outcome) error {
	return nil
}

func (*wrongCompletionHooks) Finalize(context.Context, *hookInput, *hookOutput, error) error {
	return nil
}

func (*optionalEntityHooks) AfterSequence(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput]) error {
	return nil
}
func (*optionalEntityHooks) AfterQueue(context.Context, *hookEntity, h.LifecycleContext[hookEntity, h.NoParent, hookOutput]) error {
	return nil
}

type wrongSequenceHooks struct{ rootEntityHooks }

func (*wrongSequenceHooks) AfterSequence(*hookEntity) error { return nil }

type wrongQueueHooks struct{ rootEntityHooks }

func (*wrongQueueHooks) AfterQueue(context.Context, *hookEntity, h.LifecycleContext[hookEntity, hookParent, hookOutput]) error {
	return nil
}

func TestEntityHookOptionalContracts(t *testing.T) {
	for _, tc := range []struct {
		value any
		fail  bool
	}{{rootEntityHooks{}, false}, {optionalEntityHooks{}, false}, {recoveryEntityHooks{}, false}, {transactionRetryHooks{}, false}, {wrongTransactionRetryHooks{}, true}, {wrongRecoveryHooks{}, true}, {wrongSequenceHooks{}, true}, {wrongQueueHooks{}, true}, {wrongCompletionHooks{}, true}} {
		typ := reflect.TypeOf(tc.value)
		t.Run(typ.Name(), func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: typ.PkgPath()})
			if err != nil {
				t.Fatal(err)
			}
			_, err = (EntityHookCompiler{Types: resolver}).Compile(EntityHookRequest{Hook: typ.Name(), Entity: typ.PkgPath() + ".hookEntity", Input: typ.PkgPath() + ".hookInput", Output: typ.PkgPath() + ".hookOutput"})
			if (err != nil) != tc.fail {
				t.Fatalf("error=%v", err)
			}
		})
	}
}
