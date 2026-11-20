package runtime_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	xhandler "github.com/viant/xdatly/handler"
)

type authorizedAssociationFixture struct {
	xhandler.AssociationDeltaLookup
	target xhandler.ComponentTarget
	active bool
}

func (f authorizedAssociationFixture) ActiveAssociationDelta(target xhandler.ComponentTarget) bool {
	return f.active && f.target == target
}
func (f authorizedAssociationFixture) LookupAssociationDelta(target xhandler.ComponentTarget, holder reflect.Type, field, path string) (xhandler.AssociationDelta, bool) {
	return nil, false
}

func TestExplicitAssociationRechecksPretransactionAuthorization(t *testing.T) {
	for _, active := range []bool{true, false} {
		t.Run(map[bool]string{true: "submitted", false: "omitted"}[active], func(t *testing.T) {
			var revoked bool
			r, calls, business, _ := newContractReuseFixture(t, func(context.Context, *reuseAuthInput) error {
				if revoked {
					return errors.New("permission revoked before mutation")
				}
				return nil
			})
			target := contractReuseTarget()
			portable := xhandler.ComponentTarget{Scope: target.Component.Scope, Name: target.Component.Name, Method: target.Route.Method, Path: target.Route.Path}
			ctx := exec.CaptureContracts(t.Context())
			scope := contractReuseScope("unchanged header")
			defer scope.Close()
			effective, err := r.ResolveContract(ctx, target, scope)
			require.NoError(t, err)
			exec.PublishContract(ctx, target, effective)
			require.EqualValues(t, 1, calls.Load())
			revoked = true
			ctx = context.WithValue(ctx, xhandler.AssociationDeltaLookupKey, authorizedAssociationFixture{target: portable, active: active})
			ctx = exec.WithTransactionIsolation(ctx, exec.IsolationSerializable)
			_, err = r.InvokeComponent(ctx, exec.ComponentRequest{Target: target, Providers: scope.Providers()})
			if active {
				require.ErrorContains(t, err, "permission revoked")
				require.EqualValues(t, 2, calls.Load())
				require.Zero(t, business.Load())
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 1, calls.Load())
				require.EqualValues(t, 1, business.Load())
			}
		})
	}
}
