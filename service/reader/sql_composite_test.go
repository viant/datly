package reader

import (
	"context"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
)

func TestBuilder_Build_CompositeColumnsIn_SQLite(t *testing.T) {
	aView := view.NewView("adobe", "adobe",
		view.WithConnector(view.NewConnector("test", "sqlite3", ":memory:")),
		view.WithColumns(view.Columns{
			&view.Column{Name: "ADVERTISER_ID", DataType: "int"},
			&view.Column{Name: "DMP_ADOBE_VALUE", DataType: "string"},
			&view.Column{Name: "ID", DataType: "int"},
		}),
	)
	require.NoError(t, aView.Init(context.Background(), view.EmptyResource()))

	query, err := NewBuilder().Build(context.Background(),
		WithBuilderView(aView),
		WithBuilderStatelet(view.NewStatelet()),
		WithBuilderBatchData(&view.BatchData{
			ColumnNames:          []string{"ADVERTISER_ID", "DMP_ADOBE_VALUE"},
			CompositeValues:      [][]interface{}{{101, "A"}, {202, "B"}},
			CompositeValuesBatch: [][]interface{}{{101, "A"}, {202, "B"}},
		}),
	)
	require.NoError(t, err)
	require.NotNil(t, query)
	assert.Contains(t, query.SQL, `(ADVERTISER_ID, DMP_ADOBE_VALUE) IN ((?, ?), (?, ?))`)
	assert.Equal(t, []interface{}{101, "A", 202, "B"}, query.Args)
}
