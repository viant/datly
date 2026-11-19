package reader

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

func TestBuildParametrizedSQL_UsesExplicitRootQuery(t *testing.T) {
	type row struct {
		ID int `sqlx:"id"`
	}
	aView := &view.View{Name: "dynamic", Schema: state.NewSchema(reflect.TypeOf([]*row{}))}
	aView.Schema.Cardinality = state.Many
	dest := []*row{}
	collector := view.NewCollector(aView.Schema.Slice(), aView, &dest, nil, false)
	session, err := NewSession(&dest, aView, WithQuery("SELECT id FROM source WHERE tenant_id = ?", 29))
	require.NoError(t, err)

	actual, matcher, err := New().buildParametrizedSQL(context.Background(), aView, view.NewStatelet(), nil, collector, session, nil)
	require.NoError(t, err)
	require.Nil(t, matcher)
	assert.Equal(t, "SELECT id FROM source WHERE tenant_id = ?", actual.SQL)
	assert.Equal(t, []interface{}{29}, actual.Args)

	actual.Args[0] = 30
	assert.Equal(t, 29, session.Query.Args[0], "reader owns a defensive copy of explicit query arguments")
}
