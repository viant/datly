package view

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestView_ColumnByName_UsesIndexedLookup(t *testing.T) {
	aView := NewView("disqualified", "disqualified",
		WithConnector(NewConnector("test", "sqlite3", ":memory:")),
		WithColumns(Columns{
			&Column{Name: "TAXONOMY_ID", DataType: "int", Tag: `source:"SEGMENT_ID"`},
			&Column{Name: "IS_DISQUALIFIED", DataType: "int"},
		}),
	)
	require.NoError(t, aView.Init(context.Background(), EmptyResource()))

	column, ok := aView.ColumnByName("SEGMENT_ID")
	require.True(t, ok)
	require.Equal(t, "TAXONOMY_ID", column.Name)

	column, ok = aView.ColumnByName("taxonomy_id")
	require.True(t, ok)
	require.Equal(t, "TAXONOMY_ID", column.Name)
}
