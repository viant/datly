package dql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBorrowedSQLRowLiteralDeclaration(t *testing.T) {
	declaration := "#setting($_ = $borrow_sql_row('AdOrders/Audience/Creatives','example.com/app/patch','Record','Private.dql','Private','Records'))"
	source := "#package('example.com/app/patch')\n#setting($_ = $route('/public','PATCH'))\n" + declaration + "\nSELECT id FROM records"
	c, err := parseComponentSource("example.com/app/source", "Public", source)
	require.NoError(t, err)
	require.Len(t, c.Settings.Generation.BorrowedSQLRows, 1)
	r := c.Settings.Generation.BorrowedSQLRows[0]
	require.Equal(t, "Private", r.OwnerName)
	require.Equal(t, declaration, source[r.SourceStart:r.SourceEnd])
	clone := c.Clone()
	clone.Settings.Generation.BorrowedSQLRows[0].OwnerName = "Changed"
	require.Equal(t, "Private", r.OwnerName)
	require.False(t, c.Settings.Generation.IsZero())
}

func TestBorrowedSQLRowRejectsAmbiguousAndNonliteralAuthoring(t *testing.T) {
	base := "#setting($_ = $borrow_sql_row('AdOrders/Audience/Creatives','example.com/app/patch','Record','Private.dql','Private','Records'))"
	for name, declaration := range map[string]string{
		"argument count":  strings.Replace(base, ",'Records'", "", 1),
		"computed":        strings.Replace(base, "'Record'", "$Record", 1),
		"whitespace":      strings.Replace(base, "'Record'", "' Record'", 1),
		"not exported":    strings.Replace(base, "'Record'", "'record'", 1),
		"qualified name":  strings.Replace(base, "'Record'", "'rows.Record'", 1),
		"owner traversal": strings.Replace(base, "'Private.dql'", "'../Private.dql'", 1),
		"owner absolute":  strings.Replace(base, "'Private.dql'", "'/Private.dql'", 1),
		"slot index":      strings.Replace(base, "AdOrders/Audience", "AdOrders[0]/Audience", 1),
		"empty slot":      strings.Replace(base, "'Records'", "''", 1),
		"local package":   strings.Replace(base, "'example.com/app/patch'", "'patch'", 1),
		"duplicate":       base + "\n" + base,
		"modifier":        strings.Replace(base, "))", ").Anything())", 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseComponentSource("example.com/app/source", "Public", "#setting($_ = $route('/public','PATCH'))\n"+declaration+"\nSELECT id FROM records")
			require.Error(t, err)
		})
	}
}
