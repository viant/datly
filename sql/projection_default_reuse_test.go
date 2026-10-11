package sql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
)

const defaultNullableSource = "SELECT records.* FROM (SELECT id, amount FROM spend) records"

func TestDefaultNullableDerivedProjectionPreservesRowsAndMembership(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	require.NoError(t, h.ExecStatements(context.Background(),
		"CREATE TABLE spend(id INTEGER, amount INTEGER)",
		"INSERT INTO spend VALUES(1,NULL),(2,7)"))
	p, err := (SelectorProjection{SQL: defaultNullableSource, View: nullableProjectionView()}).Prepare(nil)
	require.NoError(t, err)
	require.Equal(t, defaultNullableSource, p.Source)
	rows, err := h.DB.Query(p.Render(p.Source) + " ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()
	for _, want := range [][2]int{{1, 0}, {2, 7}} {
		require.True(t, rows.Next())
		var id, amount int
		require.NoError(t, rows.Scan(&id, &amount))
		require.Equal(t, want, [2]int{id, amount})
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	for _, source := range []string{
		"SELECT records.* FROM (SELECT id, amount, amount FROM spend) records",
		"SELECT records.* FROM (SELECT missing.* FROM spend) records",
	} {
		_, err := (SelectorProjection{SQL: source, View: nullableProjectionView()}).Prepare(nil)
		require.Error(t, err, source)
	}
}

func BenchmarkDefaultNullableDerivedProjection(b *testing.B) {
	view := nullableProjectionView()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := (SelectorProjection{SQL: defaultNullableSource, View: view}).Prepare(nil); err != nil {
			b.Fatal(err)
		}
	}
}
