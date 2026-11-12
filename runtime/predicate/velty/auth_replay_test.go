package velty

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/predicatereplay"
	"github.com/viant/datly/spec"
)

func TestAuthorizationProbeReplaysOccurrencesAndGroups(t *testing.T) {
	type input struct{ Value int }
	auth := &spec.Predicate{Name: "equal", Auth: true, Group: 1, Args: []string{"r", "tenant"}}
	ordinary := &spec.Predicate{Name: "equal", Group: 1, Args: []string{"r", "ordinary"}}
	authOnly := &spec.Predicate{Name: "equal", Auth: true, Group: 2, Args: []string{"r", "scope"}}
	program, err := Compile(CompileInput{InputType: reflect.TypeFor[input](), Component: &spec.Component{Parameters: []*spec.Parameter{{Name: "Value", Predicates: []*spec.Predicate{auth, ordinary, authOnly}}}}})
	require.NoError(t, err)
	ctx, state := predicatereplay.Capture(context.Background())
	bound := &input{Value: 7}
	var args []any
	value, err := program.NewContext(ctx, predicateBinder{input: bound}, func(values ...any) { args = append(args, values...) })
	require.NoError(t, err)
	normal, err := value.(*Context).FilterGroup(1, "OR")
	require.NoError(t, err)
	require.Contains(t, normal, "tenant")
	require.Contains(t, normal, "ordinary")
	require.Equal(t, []any{7, 7}, args)
	_, err = value.(*Context).FilterGroup(2, "OR")
	require.NoError(t, err)
	require.True(t, state.HasAuthorization())
	require.True(t, state.HasPredicate(ordinary))
	// Mutating this test's input proves replay uses the already-bound evaluation,
	// rather than recomputing ordinary predicates during diagnostic execution.
	bound.Value = 99
	args = nil
	value, err = program.NewContext(state.Probe(context.Background()), predicateBinder{input: bound}, func(values ...any) { args = append(args, values...) })
	require.NoError(t, err)
	probe, err := value.(*Context).FilterGroup(1, "OR")
	require.NoError(t, err)
	require.NotContains(t, probe, "tenant")
	require.Contains(t, probe, "ordinary")
	require.Equal(t, []any{7}, args)
	empty, err := value.(*Context).FilterGroup(2, "OR")
	require.NoError(t, err)
	require.Empty(t, empty)
	combined := value.(*Context).Builder().CombineAnd(probe, empty).Build("WHERE")
	require.Contains(t, combined, "ordinary")
	require.NotContains(t, combined, "scope")
	require.NotContains(t, combined, " OR ")
}

func TestUnusedAuthorizationOccurrenceDoesNotActivateRootProbe(t *testing.T) {
	type input struct{ Value int }
	program, err := Compile(CompileInput{InputType: reflect.TypeFor[input](), Component: &spec.Component{Parameters: []*spec.Parameter{{Name: "Value", Predicates: []*spec.Predicate{{Name: "equal", Group: 0, Args: []string{"r", "id"}}, {Name: "equal", Group: 1, Auth: true, Args: []string{"child", "tenant"}}}}}}})
	require.NoError(t, err)
	ctx, state := predicatereplay.Capture(context.Background())
	value, err := program.NewContext(ctx, predicateBinder{input: &input{Value: 7}}, nil)
	require.NoError(t, err)
	_, err = value.(*Context).FilterGroup(0, "AND")
	require.NoError(t, err)
	require.False(t, state.HasAuthorization())
}

func TestProbeCaptureDoesNotInheritAcrossPrograms(t *testing.T) {
	type input struct{ Value int }
	compile := func(auth bool) *Program {
		program, err := Compile(CompileInput{InputType: reflect.TypeFor[input](), Component: &spec.Component{Parameters: []*spec.Parameter{{Name: "Value", Predicates: []*spec.Predicate{{Name: "equal", Auth: auth, Args: []string{"r", "id"}}}}}}})
		require.NoError(t, err)
		return program
	}
	root, child := compile(false), compile(true)
	ctx, state := predicatereplay.Capture(context.Background())
	binder := predicateBinder{input: &input{Value: 7}}
	rootContext, err := root.NewContext(ctx, binder, nil)
	require.NoError(t, err)
	_, err = rootContext.(*Context).FilterGroup(0, "AND")
	require.NoError(t, err)
	childContext, err := child.NewContext(ctx, binder, nil)
	require.NoError(t, err)
	_, err = childContext.(*Context).FilterGroup(0, "AND")
	require.NoError(t, err)
	require.False(t, state.HasAuthorization(), "child authorization must not classify the parent root")
	// A reentrant native reader also shadows capture before using the same program.
	nestedContext, err := root.NewContext(predicatereplay.Without(ctx), binder, nil)
	require.NoError(t, err)
	_, err = nestedContext.(*Context).FilterGroup(0, "AND")
	require.NoError(t, err)
	probe, err := root.NewContext(state.Probe(context.Background()), binder, nil)
	require.NoError(t, err)
	_, err = probe.(*Context).FilterGroup(0, "AND")
	require.NoError(t, err)
	_, err = probe.(*Context).FilterGroup(0, "AND")
	require.ErrorContains(t, err, "differs from the authorized evaluation")
}
