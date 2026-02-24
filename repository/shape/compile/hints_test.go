package compile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape/plan"
)

func TestExtractViewHints_WithQuotedConnector(t *testing.T) {
	dql := "SELECT use_connector(match, 'bq_sitemgmt_match'), use_connector(site, \"ci_ads\"), allow_nulls(match), set_limit(match, 0)"
	hints := extractViewHints(dql)
	require.Len(t, hints, 2)
	assert.Equal(t, "bq_sitemgmt_match", hints["match"].Connector)
	assert.Equal(t, "ci_ads", hints["site"].Connector)
	require.NotNil(t, hints["match"].AllowNulls)
	assert.True(t, *hints["match"].AllowNulls)
	require.NotNil(t, hints["match"].NoLimit)
	assert.True(t, *hints["match"].NoLimit)
}

func TestApplyViewHints_Metadata(t *testing.T) {
	trueValue := true
	result := &plan.Result{
		Views: []*plan.View{
			{Name: "match", Table: "MATCH"},
		},
	}
	applyViewHints(result, map[string]viewHint{
		"match": {
			Connector:  "ci_ads",
			AllowNulls: &trueValue,
			NoLimit:    &trueValue,
		},
	})
	require.Len(t, result.Views, 1)
	assert.Equal(t, "ci_ads", result.Views[0].Connector)
	require.NotNil(t, result.Views[0].AllowNulls)
	assert.True(t, *result.Views[0].AllowNulls)
	require.NotNil(t, result.Views[0].SelectorNoLimit)
	assert.True(t, *result.Views[0].SelectorNoLimit)
}
