package compile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/view/state"
)

func TestExtractInlineParamHints(t *testing.T) {
	sql := `SELECT * FROM VENDOR t WHERE t.ID = $vendorID /* {"Kind": "header", "Location": "Vendor-Id"} */`
	hints := extractInlineParamHints(sql)
	require.Contains(t, hints, "vendorID")
	assert.Equal(t, "header", hints["vendorID"].Kind)
	assert.Equal(t, "Vendor-Id", hints["vendorID"].Location)
}

func TestApplyInlineParamHints(t *testing.T) {
	sql := `WHERE t.ID = $vendorID /* {"Kind": "header", "Location": "Vendor-Id"} */`
	result := &plan.Result{
		States: []*plan.State{
			{
				Parameter: state.Parameter{
					Name: "vendorID",
					In:   &state.Location{Kind: state.KindQuery, Name: "vendorID"},
				},
			},
		},
	}
	applyInlineParamHints(sql, result)
	require.Len(t, result.States, 1)
	assert.Equal(t, state.KindHeader, result.States[0].In.Kind)
	assert.Equal(t, "Vendor-Id", result.States[0].In.Name)
}
