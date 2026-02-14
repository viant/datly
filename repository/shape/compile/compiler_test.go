package compile

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/plan"
)

func TestDQLCompiler_Compile(t *testing.T) {
	compiler := New()
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: "SELECT id FROM ORDERS t"})
	require.NoError(t, err)
	require.NotNil(t, res)

	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.Len(t, planned.Views, 1)
	view := planned.Views[0]
	assert.Equal(t, "t", view.Name)
	assert.Equal(t, "ORDERS", view.Table)
	assert.Equal(t, "many", view.Cardinality)
}

func TestDQLCompiler_Compile_EmptyDQL(t *testing.T) {
	compiler := New()
	_, err := compiler.Compile(context.Background(), &shape.Source{Name: "x"})
	require.Error(t, err)
	assert.ErrorIs(t, err, shape.ErrNilDQL)
}

func TestDQLCompiler_Compile_WithPreamble_NoPanic(t *testing.T) {
	compiler := New()
	dql := `
/* metadata */
#set($_ = $A<string>(query/a).Optional())
SELECT id
`
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "sample_report", DQL: dql})
	require.NoError(t, err)
	require.NotNil(t, res)

	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.Len(t, planned.Views, 1)
	assert.Equal(t, "sample_report", planned.Views[0].Name)
	assert.Equal(t, "sample_report", planned.Views[0].Table)
}

func TestDQLCompiler_Compile_PropagatesTypeContext(t *testing.T) {
	compiler := New()
	dql := `
#set($_ = $package('mdp/performance'))
#set($_ = $import('perf', 'github.com/acme/mdp/performance'))
SELECT id FROM ORDERS t`
	res, err := compiler.Compile(context.Background(), &shape.Source{Name: "orders_report", DQL: dql})
	require.NoError(t, err)
	require.NotNil(t, res)

	planned, ok := res.Plan.(*plan.Result)
	require.True(t, ok)
	require.NotNil(t, planned.TypeContext)
	assert.Equal(t, "mdp/performance", planned.TypeContext.DefaultPackage)
	require.Len(t, planned.TypeContext.Imports, 1)
	assert.Equal(t, "perf", planned.TypeContext.Imports[0].Alias)
}
