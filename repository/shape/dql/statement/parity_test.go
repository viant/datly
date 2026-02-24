package statement

import (
	"testing"

	"github.com/stretchr/testify/assert"
	legacy "github.com/viant/datly/internal/translator/parser"
)

func TestStatements_ParityWithLegacyScanner(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
	}{
		{name: "read", sql: "SELECT id FROM orders"},
		{name: "exec update", sql: "UPDATE orders SET id = 1"},
		{name: "mixed", sql: "SELECT id FROM orders\nUPDATE orders SET id = 1"},
		{name: "service insert", sql: `$sql.Insert("orders", $rec)`},
		{name: "nop", sql: `$Nop($x)`},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			current := New(testCase.sql)
			old := legacy.NewStatements(testCase.sql)
			assert.Equal(t, len(old), len(current))
			for i := 0; i < len(old) && i < len(current); i++ {
				assert.Equal(t, old[i].IsExec, current[i].IsExec)
				assert.Equal(t, old[i].Start, current[i].Start)
				assert.Equal(t, old[i].End, current[i].End)
			}
		})
	}
}
