package expand

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type compositeBatch struct {
	rows [][]interface{}
}

type mockParentSource struct{}

func (m *mockParentSource) Db() (*sql.DB, error) { return nil, nil }
func (m *mockParentSource) ViewName() string     { return "test" }
func (m *mockParentSource) TableAlias() string   { return "t" }
func (m *mockParentSource) TableName() string    { return "TEST" }
func (m *mockParentSource) ResultLimit() int     { return 100 }

func (b *compositeBatch) ColIn() []interface{}              { return nil }
func (b *compositeBatch) ColInBatch() []interface{}         { return nil }
func (b *compositeBatch) CompositeIn() [][]interface{}      { return b.rows }
func (b *compositeBatch) CompositeInBatch() [][]interface{} { return b.rows }
func (b *compositeBatch) HasComposite() bool                { return len(b.rows) > 0 }

func TestViewContext_ParentCompositeJoinOn(t *testing.T) {
	viewCtx := NewViewContext(&mockParentSource{}, nil, &compositeBatch{
		rows: [][]interface{}{
			{101, "A"},
			{202, "B"},
		},
	}, &DataUnit{})
	require.NotNil(t, viewCtx)
	require.NotNil(t, viewCtx.DataUnit)

	sqlFragment, err := viewCtx.ParentCompositeJoinOn("AND", "t.advertiser_id", "t.val")
	require.NoError(t, err)
	assert.Equal(t, "AND (t.advertiser_id, t.val) IN ((?, ?), (?, ?))", sqlFragment)
	assert.Equal(t, []interface{}{101, "A", 202, "B"}, viewCtx.DataUnit.ParamsGroup)
}

func TestViewContext_ParentJoinOn_CompositeArgs(t *testing.T) {
	viewCtx := NewViewContext(&mockParentSource{}, nil, &compositeBatch{
		rows: [][]interface{}{
			{101, "A"},
			{202, "B"},
		},
	}, &DataUnit{})
	require.NotNil(t, viewCtx)

	sqlFragment, err := viewCtx.ParentJoinOn("AND", "t.advertiser_id", "t.val")
	require.NoError(t, err)
	assert.Equal(t, "AND (t.advertiser_id, t.val) IN ((?, ?), (?, ?))", sqlFragment)
	assert.Equal(t, []interface{}{101, "A", 202, "B"}, viewCtx.DataUnit.ParamsGroup)
}

func TestViewContext_ParentCompositeJoinOn_EmptyRows(t *testing.T) {
	viewCtx := NewViewContext(&mockParentSource{}, nil, &compositeBatch{}, &DataUnit{})
	require.NotNil(t, viewCtx)

	sqlFragment, err := viewCtx.ParentCompositeJoinOn("AND", "t.advertiser_id", "t.val")
	require.NoError(t, err)
	assert.True(t, strings.Contains(sqlFragment, "1 = 0"))
}
