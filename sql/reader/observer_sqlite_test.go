package reader_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	sqlxio "github.com/viant/sqlx/io"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache/afs"
)

func TestSQLXColumnsObserverSQLite(t *testing.T) {
	type row struct {
		ID   int    `sqlx:"id"`
		Name string `sqlx:"name"`
	}
	for _, tc := range []struct {
		name, SQL string
		columns   []string
		fail      bool
	}{{"full", "SELECT id,name FROM records", []string{"id", "name"}, false}, {"projection", "SELECT name FROM records", []string{"name"}, false}, {"observer_error", "SELECT id FROM records", []string{"id"}, true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,'one')"); err != nil {
				t.Fatal(err)
			}
			service, err := afs.NewCache(t.TempDir(), time.Minute, "observer", nil)
			if err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 2; pass++ {
				calls := 0
				reader, err := sqlxread.New(ctx, db.DB, tc.SQL, func() any { return &row{} }, sqlxread.WithCache(service), sqlxread.WithColumnsObserver(func(columns []sqlxio.Column) error {
					calls++
					var names []string
					for _, column := range columns {
						names = append(names, column.Name())
					}
					if !reflect.DeepEqual(names, tc.columns) {
						return fmt.Errorf("columns=%v,want %v", names, tc.columns)
					}
					if tc.fail {
						return fmt.Errorf("observer rejected schema")
					}
					return nil
				}))
				if err != nil {
					t.Fatal(err)
				}
				err = reader.QueryAll(ctx, func(any) error { return nil })
				if reader.Stmt() != nil {
					_ = reader.Stmt().Close()
				}
				if tc.fail {
					if err == nil {
						t.Fatal("observer error lost")
					}
					return
				}
				if err != nil || calls != 1 {
					t.Fatalf("pass=%d calls=%d error=%v", pass, calls, err)
				}
				if pass == 0 {
					db.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,name FROM records"}, []row{{1, "one"}})
					if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
