package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

// This fixture deliberately uses only baseline SQLX APIs and can run against
// both clean SQLX and the candidate replacement through external modfiles.
func TestOriginalSQLXScalarMatcherCompatibilitySQLite(t *testing.T) {
	type row struct {
		Name string `sqlx:"name"`
	}
	for _, tc := range []struct {
		name, SQL     string
		offset, limit int
		want          []row
	}{
		{"single_key_projection", "SELECT name FROM records WHERE group_id=? ORDER BY id", 0, 0, []row{{"first"}, {"second"}}},
		{"single_key_SQL_window", "SELECT name FROM records WHERE group_id=? ORDER BY id LIMIT 1 OFFSET 1", 1, 1, []row{{"second"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(group_id INTEGER,id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,1,'first'),(1,2,'second'),(2,3,'other')"); err != nil {
				t.Fatal(err)
			}
			reader, err := sqlxread.New(ctx, db.DB, tc.SQL, func() any { return &row{} }, sqlxread.WithInMatcher(&cache.ParmetrizedQuery{By: "group_id", In: []any{1}, Offset: tc.offset, Limit: tc.limit}))
			if err != nil {
				t.Fatal(err)
			}
			actual := []row{}
			err = reader.QueryAll(ctx, func(value any) error { actual = append(actual, *value.(*row)); return nil }, 1)
			if reader.Stmt() != nil {
				_ = reader.Stmt().Close()
			}
			if err != nil || !reflect.DeepEqual(actual, tc.want) {
				t.Fatalf("rows=%v,want=%v,error=%v", actual, tc.want, err)
			}
		})
	}
}
