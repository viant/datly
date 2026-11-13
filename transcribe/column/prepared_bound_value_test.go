package column

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestPreparedBoundValuePrelude(t *testing.T) {
	source := `#set($selected = -1)
#if($Unsafe.Has.Ids)
#set($selected = $Unsafe.Ids[0])
#end
SELECT r.id, w.weight FROM records r LEFT JOIN record_weights w ON w.record_id=r.id AND w.owner_id=$criteria.AppendBinding($selected)`
	view := &spec.View{Name: "Records", Namespace: "r", Source: &spec.ViewSource{SQL: source}}
	component := &spec.Component{RootView: view, Parameters: []*spec.Parameter{{Name: "Ids", TypeExpr: "[]int", Source: spec.BindSource{Kind: "query", Name: "ids"}}}}
	calls := 0
	err := RefinePrepared(t.Context(), component, nil, func(_ context.Context, request PreparedColumnRequest) (*PreparedColumnResult, error) {
		calls++
		require.Equal(t, source, request.Source.SQL)
		return &PreparedColumnResult{Columns: []*spec.Column{{Name: "id", Source: "id", Type: spec.TypeRef{Name: "int"}}, {Name: "weight", Source: "weight", Type: spec.TypeRef{Name: "int"}}}}, nil
	}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.Equal(t, source, view.Source.SQL)
	require.Len(t, view.Columns, 2)
}
