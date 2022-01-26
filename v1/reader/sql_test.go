package reader

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuilder_Build(t *testing.T) {
	testCases := []struct {
		description string
		tableName   string
		columns     []string
		expectedSql string
	}{
		{
			description: "specified columns",
			tableName:   "FOOS",
			columns:     []string{"name", "price", "id"},
			expectedSql: "SELECT name, price, id FROM FOOS",
		},
	}

	for _, testCase := range testCases {
		builder := NewBuilder()
		assert.Equal(t, testCase.expectedSql, builder.Build(testCase.columns, testCase.tableName))
	}
}
