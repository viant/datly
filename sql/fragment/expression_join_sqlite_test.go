package fragment

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestExpressionBoundLeftJoinKeepsUnmatchedRows(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(),
		`CREATE TABLE assets(id INTEGER PRIMARY KEY)`,
		`CREATE TABLE observations(asset_id INTEGER, phase TEXT, amount INTEGER)`,
		`INSERT INTO assets VALUES(1),(2)`,
		`INSERT INTO observations VALUES(1,'current',100),(1,'prior',80),(2,'current',200)`,
	))
	type row struct {
		ID     int  `sqlx:"id"`
		Amount *int `sqlx:"amount"`
	}
	prior := 80
	for _, tc := range []struct {
		name, phase string
		expected    []row
	}{
		{"prior", "prior", []row{{1, &prior}, {2, nil}}},
		{"value stays bound", "prior' OR 1=1 --", []row{{1, nil}, {2, nil}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bindings := NewBindings()
			condition, err := New(bindings).Expression("p.phase = ?", tc.phase)
			require.NoError(t, err)
			require.Equal(t, "p.phase = ?", condition)
			require.Equal(t, []any{tc.phase}, bindings.Args())
			h.AssertQuery(t, t.Context(), sqlite.Query{
				SQL:  "SELECT a.id, p.amount FROM assets a LEFT JOIN observations p ON p.asset_id = a.id AND " + condition + " ORDER BY a.id",
				Args: bindings.Args(),
			}, tc.expected)
		})
	}
}

func TestExpressionColumnComparisonNeedsNoValueSlot(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(),
		`CREATE TABLE assets(id INTEGER, owner_id INTEGER)`,
		`INSERT INTO assets VALUES(1,1),(2,2),(3,99)`,
	))
	bindings := NewBindings()
	condition, err := New(bindings).Expression("a.id = a.owner_id", true)
	require.NoError(t, err)
	require.Empty(t, bindings.Args())
	h.AssertQuery(t, t.Context(), sqlite.Query{SQL: "SELECT a.id FROM assets a WHERE " + condition + " ORDER BY a.id"}, []struct{ ID int }{{1}, {2}})
}
