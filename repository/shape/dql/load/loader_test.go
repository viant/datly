package load

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type sampleView struct {
	ID int
}

type manyHolder struct {
	Rows *[]sampleView `view:"rows,table=CI_SAMPLE,connector=ci_ads,partitioner=custom.Partitioner,concurrency=4,relationalConcurrency=2" sql:"SELECT ID FROM CI_SAMPLE"`
}

type oneHolder struct {
	Row *sampleView `view:"row,table=CI_SAMPLE,connector=ci_ads"`
}

func TestFromHolderStruct_ManyCardinality(t *testing.T) {
	artifact, err := FromHolderStruct(context.Background(), &manyHolder{})
	require.NoError(t, err)
	require.NotNil(t, artifact)

	resource, ok := artifact.Canonical["Resource"].(map[string]any)
	require.True(t, ok)
	views, ok := resource["Views"].([]any)
	require.True(t, ok)
	require.Len(t, views, 1)
	view, ok := views[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "rows", view["Name"])
	require.Equal(t, "CI_SAMPLE", view["Table"])
	require.Equal(t, "ci_ads", view["ConnectorRef"])
	require.Equal(t, "Rows", view["Holder"])
	require.Equal(t, "many", view["Cardinality"])
	require.Equal(t, "custom.Partitioner", view["Partitioner"])
	require.EqualValues(t, 4, view["PartitionedConcurrency"])
	require.EqualValues(t, 2, view["RelationalConcurrency"])
}

func TestFromHolderStruct_OneCardinality(t *testing.T) {
	artifact, err := FromHolderStruct(context.Background(), &oneHolder{})
	require.NoError(t, err)
	require.NotNil(t, artifact)

	resource, ok := artifact.Canonical["Resource"].(map[string]any)
	require.True(t, ok)
	views, ok := resource["Views"].([]any)
	require.True(t, ok)
	require.Len(t, views, 1)
	view, ok := views[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "row", view["Name"])
	require.Equal(t, "one", view["Cardinality"])
}
