package contract

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	xcontract "github.com/viant/xdatly/contract"
)

// Use the existing role policy with a fresh external owner, retaining the
// canonical inline input and the planner's ordinary variant cache.
type externalLifecyclePolicy struct{}

func (*externalLifecyclePolicy) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	return (&policyInput{}).Contract(ctx, binder)
}

func TestLifecycleContractOnAnonymousInput(t *testing.T) {
	typ := reflect.StructOf([]reflect.StructField{{Name: "Value", Type: reflect.TypeFor[string]()}, {Name: "Other", Type: reflect.TypeFor[string]()}})
	p, err := New(typ, 8, reflect.TypeFor[externalLifecyclePolicy]())
	require.NoError(t, err)
	require.True(t, p.Dynamic())
	for _, role := range []string{"hide", "admin", "hide"} {
		effective, err := p.Resolve(t.Context(), policyBinder(t, role))
		require.NoError(t, err)
		field, ok := effective.FieldByName("Value")
		require.True(t, ok)
		if role == "hide" {
			require.Equal(t, "-", field.Tag.Get("json"))
		} else {
			require.Equal(t, typ, effective)
		}
	}
	_, err = New(reflect.TypeFor[policyInput](), 8, reflect.TypeFor[externalLifecyclePolicy]())
	require.Error(t, err)
}
