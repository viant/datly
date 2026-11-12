package view

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProjectionFieldForColumn_DoesNotSetGroupedSemanticKeys(t *testing.T) {
	dimension := ProjectionFieldForColumn(&Column{Name: "audience_id", Groupable: true})

	assert.Empty(t, dimension.DimensionKey)
	assert.Empty(t, dimension.MeasureKey)

	measure := ProjectionFieldForColumn(&Column{Name: "spend", Aggregate: true})

	assert.Empty(t, measure.DimensionKey)
	assert.Empty(t, measure.MeasureKey)
}

func TestProjectionFieldForColumn_KeepsSourceOutOfLookup(t *testing.T) {
	field := ProjectionFieldForColumn(&Column{
		Name:           "campaign_id",
		DatabaseColumn: "CAMPAIGN_ID",
		Tag:            `source:"ID"`,
	})

	assert.Equal(t, "ID", field.Source)
	assert.NotContains(t, field.Lookup, "ID")
	assert.NotContains(t, field.Lookup, "id")
	assert.Contains(t, field.Lookup, "campaign_id")
	assert.Contains(t, field.Lookup, "campaignid")
}

func TestProjectionFieldsForNames_NonGroupedViewDoesNotSetGroupedSemanticKeys(t *testing.T) {
	aView := NewView("events", "events",
		WithConnector(NewConnector("test", "sqlite3", ":memory:")),
		WithColumns(Columns{
			{Name: "order_id", DataType: "int", Groupable: true},
			{Name: "spend", DataType: "float", Aggregate: true},
		}),
	)
	require.NoError(t, aView.Init(context.Background(), EmptyResource()))
	fields, err := ProjectionFieldsForNames(aView, []string{"order_id", "spend"})
	require.NoError(t, err)
	require.Len(t, fields, 2)

	assert.Empty(t, fields[0].DimensionKey)
	assert.Empty(t, fields[0].MeasureKey)
	assert.Empty(t, fields[1].DimensionKey)
	assert.Empty(t, fields[1].MeasureKey)
}

func TestProjectionFieldsForNames_GroupedViewTreatsNonGroupableColumnsAsMeasures(t *testing.T) {
	aView := NewView("linePeriodSummary", "line_period_summary",
		WithConnector(NewConnector("test", "sqlite3", ":memory:")),
		WithGroupable(true),
		WithColumns(Columns{
			{Name: "audience_id", DataType: "int", Groupable: true},
			{Name: "SPEND", DataType: "float"},
			{Name: "PERIOD_ECPM", DataType: "float"},
		}),
	)
	require.NoError(t, aView.Init(context.Background(), EmptyResource()))
	fields, err := ProjectionFieldsForNames(aView, []string{"audience_id", "spend", "period_ecpm"})
	require.NoError(t, err)
	require.Len(t, fields, 3)

	assert.Equal(t, "audience_id", fields[0].DimensionKey)

	assert.Empty(t, fields[1].DimensionKey)
	assert.Equal(t, "SPEND", fields[1].MeasureKey)

	assert.Empty(t, fields[2].DimensionKey)
	assert.Equal(t, "PERIOD_ECPM", fields[2].MeasureKey)
}
