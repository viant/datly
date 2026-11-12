package sql

import (
	dbsql "database/sql"
	"sort"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/io/read/cache"
)

func TestGroupedNullDefaultsPreserveGrainAndCacheProjection(t *testing.T) {
	db, err := dbsql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE facts(item_id INTEGER,bucket INTEGER,amount INTEGER);
INSERT INTO facts VALUES(10,NULL,4),(10,0,6);`)
	require.NoError(t, err)
	groupable := true
	view := &data.View{Spec: spec.View{Groupable: &groupable}, Columns: []*data.Column{
		{Name: "item_id", Column: "item_id", Nullable: true, NullFallback: "0", Groupable: true},
		{Name: "bucket", Column: "bucket", Nullable: true, NullFallback: "0", Groupable: true},
		{Name: "total", Column: "total", Nullable: true, NullFallback: "0"},
	}}
	inner := `SELECT f.item_id,f.bucket,SUM(f.amount) AS total FROM facts f GROUP BY f.item_id,f.bucket`
	for _, source := range []string{inner, "SELECT f.* FROM (" + inner + ") f"} {
		full, err := ApplySelectorProjection(source, nil, view)
		require.NoError(t, err)
		selected, err := ApplySelectorProjection(source, []string{"item_id", "bucket", "total"}, view)
		require.NoError(t, err)
		for _, statement := range []string{full, selected} {
			rows, err := db.Query(statement)
			require.NoError(t, err, statement)
			var totals []int
			for rows.Next() {
				var id, bucket, total int
				require.NoError(t, rows.Scan(&id, &bucket, &total))
				require.Equal(t, 10, id)
				require.Zero(t, bucket)
				totals = append(totals, total)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			sort.Ints(totals)
			require.Equal(t, []int{4, 6}, totals, statement)
		}
		stored, err := (CacheProjection{SQL: full, View: view}).Fields()
		require.NoError(t, err)
		requested, err := (CacheProjection{SQL: selected, View: view}).Fields()
		require.NoError(t, err)
		_, hit, reason, err := (cache.Projection{Stored: stored}).Indexes(requested)
		require.NoError(t, err)
		require.True(t, hit, reason)
		coarse, err := ApplySelectorProjection(source, []string{"item_id", "total"}, view)
		require.NoError(t, err)
		coarseFields, err := (CacheProjection{SQL: coarse, View: view}).Fields()
		require.NoError(t, err)
		_, hit, _, err = (cache.Projection{Stored: stored}).Indexes(coarseFields)
		require.NoError(t, err)
		require.False(t, hit, "coarser grain must not reuse warmed groups")
	}
}
