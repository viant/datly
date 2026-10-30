package fragment

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestNullableInSQLite(t *testing.T) {
	type row struct {
		ID int `sqlx:"id"`
	}
	h := sqlite.New(t)
	ctx := context.Background()
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, tenant INTEGER, segment TEXT)", "INSERT INTO records VALUES (1,7,NULL),(2,7,'Cedar'),(3,7,'null'),(4,7,'Other'),(5,8,NULL)"))
	cedar := "Cedar"
	for _, tc := range []struct {
		name      string
		selection any
		want      []row
		args      []any
	}{
		{"absent", nil, []row{{1}, {2}, {3}, {4}}, nil},
		{"empty", []*string{}, []row{{1}, {2}, {3}, {4}}, nil},
		{"NULL only", []*string{nil}, []row{{1}}, nil},
		{"mixed typed", []*string{&cedar, nil}, []row{{1}, {2}}, []any{"Cedar"}},
		{"mixed interface", []any{"Cedar", nil}, []row{{1}, {2}}, []any{"Cedar"}},
		{"literal null", []any{"null"}, []row{{3}}, []any{"null"}},
		{"bound text", []any{"Cedar') OR 1=1 --", nil}, []row{{1}}, []any{"Cedar') OR 1=1 --"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBindings()
			clause, err := New(b).NullableIn("r.segment", tc.selection)
			require.NoError(t, err)
			require.Equal(t, tc.args, b.Args())
			query := "SELECT r.id FROM records r WHERE r.tenant = 7"
			if clause != "" {
				query += " AND (" + clause + ")"
			}
			query += " ORDER BY r.id"
			h.AssertQuery(t, ctx, sqlite.Query{SQL: query, Args: b.Args()}, tc.want)
		})
	}
	b := NewBindings()
	clause, err := New(b).NullableIn("CASE WHEN r.segment='Other' THEN NULL ELSE r.segment END", []any{nil, "Cedar"})
	require.NoError(t, err)
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT r.id FROM records r WHERE r.tenant=7 AND " + clause + " ORDER BY r.id", Args: b.Args()}, []row{{1}, {2}, {4}})
	_, err = New(b).NullableIn("r.segment", "Cedar")
	require.Error(t, err)
	// Existing IN/NOT IN keep their historical NULL-dropping contract.
	clause, err = New(NewBindings()).In("segment", []any{nil})
	require.NoError(t, err)
	require.Equal(t, "1 = 0", clause)
	clause, err = New(NewBindings()).NotIn("segment", []any{nil})
	require.NoError(t, err)
	require.Equal(t, "0 = 0", clause)
}
