package compile

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/x"
)

type linkedRootType struct {
	ID int
}

type OrdersView struct {
	ID int
}

func TestDQLCompiler_Compile_UsesLinkedRootTypeForSchemaType(t *testing.T) {
	compiler := New()
	source := &shape.Source{
		Name:     "orders_report",
		Type:     reflect.TypeOf(linkedRootType{}),
		TypeName: x.NewType(reflect.TypeOf(linkedRootType{})).Key(),
		DQL:      "SELECT t.id FROM ORDERS t",
	}

	res, err := compiler.Compile(context.Background(), source)
	require.NoError(t, err)
	planned, ok := plan.ResultFrom(res)
	require.True(t, ok)
	require.NotEmpty(t, planned.Views)
	assert.Equal(t, "*compile.linkedRootType", planned.Views[0].SchemaType)
	require.NotEmpty(t, planned.Types)
	assert.Equal(t, "linkedRootType", planned.Types[0].Name)
	assert.Equal(t, "*compile.linkedRootType", planned.Types[0].DataType)
}

func TestDQLCompiler_Compile_UsesLinkedRegistryTypeForNamedView(t *testing.T) {
	compiler := New()
	registry := x.NewRegistry()
	registry.Register(x.NewType(reflect.TypeOf(OrdersView{})))
	source := &shape.Source{
		Name:         "orders",
		TypeRegistry: registry,
		DQL:          "SELECT orders.id FROM ORDERS orders",
	}

	res, err := compiler.Compile(context.Background(), source)
	require.NoError(t, err)
	planned, ok := plan.ResultFrom(res)
	require.True(t, ok)
	require.NotEmpty(t, planned.Views)
	assert.Equal(t, "*compile.OrdersView", planned.Views[0].SchemaType)

	var found *plan.Type
	for _, item := range planned.Types {
		if item != nil && item.Name == "OrdersView" {
			found = item
			break
		}
	}
	require.NotNil(t, found)
	assert.Equal(t, "*compile.OrdersView", found.DataType)
	assert.Equal(t, "compile", found.Package)
}
