package compile

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	"github.com/viant/datly/repository/shape/plan"
)

func TestApplyColumnDiscoveryPolicy_Auto_WildcardRequiresDiscovery(t *testing.T) {
	result := &plan.Result{
		Views: []*plan.View{{
			Name:        "orders",
			Mode:        "SQLQuery",
			SQL:         "SELECT * FROM ORDERS",
			FieldType:   reflect.TypeOf([]struct{ ID int }{}),
			ElementType: reflect.TypeOf(struct{ ID int }{}),
		}},
	}
	diags := applyColumnDiscoveryPolicy(result, &shape.CompileOptions{ColumnDiscoveryMode: shape.CompileColumnDiscoveryAuto})
	require.Empty(t, diags)
	require.True(t, result.ColumnsDiscovery)
	require.True(t, result.Views[0].ColumnsDiscovery)
}

func TestApplyColumnDiscoveryPolicy_Auto_NoConcreteShapeRequiresDiscovery(t *testing.T) {
	result := &plan.Result{
		Views: []*plan.View{{
			Name:        "orders",
			Mode:        "SQLQuery",
			SQL:         "SELECT id FROM ORDERS",
			FieldType:   reflect.TypeOf([]map[string]any{}),
			ElementType: reflect.TypeOf(map[string]any{}),
		}},
	}
	diags := applyColumnDiscoveryPolicy(result, &shape.CompileOptions{ColumnDiscoveryMode: shape.CompileColumnDiscoveryAuto})
	require.Empty(t, diags)
	require.True(t, result.ColumnsDiscovery)
	require.True(t, result.Views[0].ColumnsDiscovery)
}

func TestApplyColumnDiscoveryPolicy_Off_EmitsErrorWhenRequired(t *testing.T) {
	result := &plan.Result{
		Views: []*plan.View{{
			Name:        "orders",
			Mode:        "SQLQuery",
			SQL:         "SELECT * FROM ORDERS",
			FieldType:   reflect.TypeOf([]map[string]any{}),
			ElementType: reflect.TypeOf(map[string]any{}),
		}},
	}
	diags := applyColumnDiscoveryPolicy(result, &shape.CompileOptions{ColumnDiscoveryMode: shape.CompileColumnDiscoveryOff})
	require.NotEmpty(t, diags)
	assert.Equal(t, dqldiag.CodeColDiscoveryReq, diags[0].Code)
	assert.True(t, result.ColumnsDiscovery)
	assert.True(t, result.Views[0].ColumnsDiscovery)
}

func TestApplyColumnDiscoveryPolicy_On_AlwaysMarksQueryViews(t *testing.T) {
	result := &plan.Result{
		Views: []*plan.View{{
			Name:        "orders",
			Mode:        "SQLQuery",
			SQL:         "SELECT id FROM ORDERS",
			FieldType:   reflect.TypeOf([]struct{ ID int }{}),
			ElementType: reflect.TypeOf(struct{ ID int }{}),
		}},
	}
	diags := applyColumnDiscoveryPolicy(result, &shape.CompileOptions{ColumnDiscoveryMode: shape.CompileColumnDiscoveryOn})
	require.Empty(t, diags)
	assert.True(t, result.ColumnsDiscovery)
	assert.True(t, result.Views[0].ColumnsDiscovery)
}
