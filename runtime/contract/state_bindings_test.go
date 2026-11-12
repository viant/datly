package contract

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/values"
	"github.com/viant/bindly/state"
)

type preparedStateProbe struct {
	*bindly.Injector
	plan     *bindly.Plan
	bindings []bindly.BindingSpec
	calls    int
}

func (p *preparedStateProbe) BindContractState(ctx context.Context, target any, plan *bindly.Plan, bindings []bindly.BindingSpec, options ...bindly.BindOption) error {
	p.calls++
	p.plan, p.bindings = plan, bindings
	return p.Injector.Bind(ctx, target, append(options, bindly.WithPlan(plan))...)
}

func TestPreparedContractStateSharesMetadataNotInvocationValues(t *testing.T) {
	type input struct{ Role string }
	compiler, err := bindly.NewInjector()
	require.NoError(t, err)
	compilations := 0
	plans := NewStatePlansWithBindings(func(typ reflect.Type) (*bindly.Plan, []bindly.BindingSpec, error) {
		compilations++
		bindings := []bindly.BindingSpec{{Path: "Role", Location: state.Location{Kind: "header", In: "X-Role"}}}
		plan, err := compiler.CompilePlan(typ, bindings...)
		return plan, bindings, err
	})
	var compiled *bindly.Plan
	for _, role := range []string{"first", "second"} {
		scope, err := compiler.ForScope(values.New("header", map[string]any{"X-Role": role}))
		require.NoError(t, err)
		owner := &preparedStateProbe{Injector: scope}
		var actual input
		require.NoError(t, plans.Binder(owner).Bind(t.Context(), &actual))
		require.Equal(t, role, actual.Role)
		require.Len(t, owner.bindings, 1)
		require.Equal(t, "X-Role", owner.bindings[0].Location.In)
		if compiled != nil {
			require.Same(t, compiled, owner.plan)
		}
		compiled = owner.plan
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, plans.Binder(owner).Bind(ctx, &actual), context.Canceled)
		require.Equal(t, 1, owner.calls)
	}
	require.Equal(t, 1, compilations)
}
