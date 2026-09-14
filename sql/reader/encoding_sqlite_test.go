package reader_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache/afs"
)

type csvIntegerRow struct {
	Values []int `sqlx:"encoded,enc=CSV"`
}
type jsonIntegerRow struct {
	Values []int `sqlx:"encoded,enc=JSON"`
}

func TestNativeEncodedSQLXCacheSQLite(t *testing.T) {
	for _, tc := range []struct {
		name        string
		raw         any
		model, want any
		fail        bool
	}{
		{"csv", "01,02", csvIntegerRow{}, csvIntegerRow{Values: []int{1, 2}}, false},
		{"csv_null", nil, csvIntegerRow{}, csvIntegerRow{}, false},
		{"csv_empty", "", csvIntegerRow{}, csvIntegerRow{Values: []int{}}, false},
		{"csv_invalid", "1,no", csvIntegerRow{}, nil, true},
		{"json", "[1,2]", jsonIntegerRow{}, jsonIntegerRow{Values: []int{1, 2}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(encoded TEXT)"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.DB.ExecContext(ctx, "INSERT INTO records VALUES(?)", tc.raw); err != nil {
				t.Fatal(err)
			}
			service, err := afs.NewCache(t.TempDir(), time.Minute, "encoding", nil)
			if err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 2; pass++ {
				reader, err := sqlxread.New(ctx, db.DB, "SELECT encoded FROM records", func() any { return reflect.New(reflect.TypeOf(tc.model)).Interface() }, sqlxread.WithCache(service))
				if err != nil {
					t.Fatal(err)
				}
				var actual any
				err = reader.QueryAll(ctx, func(value any) error { actual = reflect.ValueOf(value).Elem().Interface(); return nil })
				if reader.Stmt() != nil {
					_ = reader.Stmt().Close()
				}
				if tc.fail {
					if err == nil {
						t.Fatal("invalid encoding accepted")
					}
					return
				}
				if err != nil || !reflect.DeepEqual(actual, tc.want) {
					t.Fatalf("pass=%d actual=%#v want=%#v error=%v", pass, actual, tc.want, err)
				}
				if pass == 0 {
					if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
