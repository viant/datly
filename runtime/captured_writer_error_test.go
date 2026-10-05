package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/capturederror"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/response"
)

// A failed child initializer returns its captured canonical result, while the
// caller's already flushed transaction still rolls back before finalization.
func TestCapturedWriterChildErrorPreservesResultAndCallerRollback(t *testing.T) {
	for _, remap := range []bool{false, true} {
		t.Run(fmt.Sprint(remap), func(t *testing.T) {
			ctx := context.Background()
			child, observed, err := capturederror.New(t, remap)
			require.NoError(t, err)
			_, err = observed.DB.Exec("CREATE TABLE caller_audit(id INTEGER PRIMARY KEY)")
			require.NoError(t, err)
			parentSpec := componentSpec("CapturedErrorCaller", "POST", "/captured-error-caller", nil)
			artifact := componentArtifact(t, parentSpec, reflect.TypeFor[struct{}](), reflect.TypeFor[capturederror.Output]())
			var childResult any
			parent := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[capturederror.Output](), DataSource: dml.Source{DB: observed.DB}, Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				value, found, err := inv.Binder.Lookup(ctx, xh.DataKey)
				if err != nil || !found {
					return nil, fmt.Errorf("caller data: %v", err)
				}
				data := value.(xh.Data)
				if err = data.Execute("INSERT INTO caller_audit VALUES(1)"); err != nil {
					return nil, err
				}
				if err = data.Flush(ctx, ""); err != nil {
					return nil, err
				}
				value, found, err = inv.Binder.Lookup(ctx, dexec.ComponentInvokerKey)
				if err != nil || !found {
					return nil, fmt.Errorf("scoped invoker: %v", err)
				}
				childResult, err = value.(dexec.ComponentInvoker).InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: child.Component.Key, Route: spec.RouteRef{Method: "PATCH", Path: "/captured-error"}}, Providers: []locator.Provider{namedProvider("body", "data", []*capturederror.Record{{ID: 7}})}})
				require.Equal(t, 0, observed.Observe().Finalizes, "child finalization must wait for caller rollback")
				return childResult, err
			})}
			runtime, err := NewRuntime([]*registry.RegisteredComponent{parent, child})
			require.NoError(t, err)
			defer runtime.Shutdown(ctx)
			value, err := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: parentSpec.Key, Route: spec.RouteRef{Method: "POST", Path: "/captured-error-caller"}}, Input: &struct{}{}})
			require.Error(t, err)
			require.True(t, errors.Is(err, capturederror.BusinessCause))
			evidence := observed.Observe()
			require.Same(t, childResult, value)
			require.Same(t, evidence.Canonical, value)
			require.Same(t, evidence.Canonical, evidence.Finalized)
			require.Equal(t, 1, evidence.Captures)
			require.Equal(t, 1, evidence.Bridges)
			require.Equal(t, 1, evidence.Finalizes)
			require.Zero(t, evidence.Executes)
			status, want := 400, "business"
			if remap {
				status, want = 409, "finalized"
			}
			require.Equal(t, status, response.ErrorStatusCode(err, 0))
			body, ok := response.ErrorBody(err)
			require.True(t, ok)
			require.Equal(t, want, body.(*capturederror.Output).Status)
			var auditRows int
			require.NoError(t, observed.DB.QueryRow("SELECT COUNT(*) FROM caller_audit").Scan(&auditRows))
			require.Zero(t, auditRows)
			observed.AssertState(t)
		})
	}
}

// Root completion SQL failures suppress the returned result. Outcome finalizers
// retain their pre-completion output, while source adapters that return nil must
// keep nil. The Init-only bridge must run in neither case.
func TestCapturedWriterLateSQLFailurePreservesExistingResultPolicy(t *testing.T) {
	for _, sourceNil := range []bool{false, true} {
		t.Run(fmt.Sprint(sourceNil), func(t *testing.T) {
			ctx := context.Background()
			child, observed, err := capturederror.NewSQLFailure(t, sourceNil)
			require.NoError(t, err)
			runtime, err := NewRuntime([]*registry.RegisteredComponent{child})
			require.NoError(t, err)
			defer runtime.Shutdown(ctx)
			// Separate invocations prove runtime reuse, not recovery-retry admission.
			for attempt := 1; attempt <= 2; attempt++ {
				value, err := runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: child.Component.Key, Route: spec.RouteRef{Method: "PATCH", Path: "/captured-error"}}, Providers: []locator.Provider{namedProvider("body", "data", []*capturederror.Record{{ID: 7}})}})
				require.Error(t, err)
				require.Contains(t, err.Error(), "UNIQUE constraint failed")
				evidence := observed.Observe()
				require.Equal(t, attempt, evidence.Captures)
				require.Equal(t, attempt, evidence.Executes)
				require.Zero(t, evidence.Bridges)
				require.Equal(t, attempt, evidence.Finalizes)
				if sourceNil {
					require.Nil(t, value)
					require.Nil(t, evidence.Finalized)
				} else {
					require.Nil(t, value)
					require.NotNil(t, evidence.Finalized)
					require.Equal(t, 7, evidence.Finalized.Data[0].ID)
				}
				observed.AssertState(t)
			}
		})
	}
}
