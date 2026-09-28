package view

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlparser"
)

func TestNewColumns_AliasPreservesSourceAndProjectedNames(t *testing.T) {
	columns := NewColumns(sqlparser.Columns{
		&sqlparser.Column{Name: "NAME", Type: "string"},
	}, map[string]*ColumnConfig{
		"NAME": {Name: "NAME", Alias: "SITE_NAME"},
	})

	require.Len(t, columns, 1)
	assert.Equal(t, "SITE_NAME", columns[0].Name)
	assert.Contains(t, columns[0].Tag, `sqlx:"NAME|SITE_NAME"`)
}
