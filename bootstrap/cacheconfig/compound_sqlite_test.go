package cacheconfig

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

func TestNativeCompoundCacheSQLite(t *testing.T) {
	type row struct {
		Tenant string `sqlx:"tenant"`
		UserID int    `sqlx:"user_id"`
		ID     int    `sqlx:"id"`
	}
	for _, warm := range []bool{false, true} {
		name := "lazy"
		if warm {
			name = "indexed"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(tenant TEXT,user_id INTEGER,id INTEGER)", "INSERT INTO records VALUES('a',1,1),('a',1,2),('b',1,3),('b',1,4),('a',2,5),('a',2,6)"); err != nil {
				t.Fatal(err)
			}
			service, err := (Config{Identity: "component/root", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
			if err != nil {
				t.Fatal(err)
			}
			const SQL = "SELECT tenant,user_id,id FROM records ORDER BY id"
			columns := []string{"tenant", "user_id"}
			if warm {
				if count, err := service.IndexBy(ctx, db.DB, "", SQL, nil, &cache.ParmetrizedQuery{ByColumns: columns}); err != nil || count != 3 {
					t.Fatalf("IndexBy=%d,%v", count, err)
				}
			}
			for _, test := range []struct {
				offset int
				want   []row
			}{{0, []row{{"a", 1, 1}, {"b", 1, 3}}}, {1, []row{{"a", 1, 2}, {"b", 1, 4}}}} {
				if test.offset == 1 {
					if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
						t.Fatal(err)
					}
				}
				matcher := &cache.ParmetrizedQuery{IdentitySQL: SQL, ByColumns: columns, InTuples: [][]any{{"a", 1}, {"b", 1}}, Offset: test.offset, Limit: 1}
				reader, err := sqlxread.New(ctx, db.DB, SQL, func() any { return &row{} }, sqlxread.WithCache(service), sqlxread.WithInMatcher(matcher))
				if err != nil {
					t.Fatal(err)
				}
				actual := []row{}
				if err := reader.QueryAll(ctx, func(value any) error { actual = append(actual, *value.(*row)); return nil }); err != nil {
					t.Fatal(err)
				}
				if reader.Stmt() != nil {
					_ = reader.Stmt().Close()
				}
				if !reflect.DeepEqual(actual, test.want) {
					t.Fatalf("offset=%d: rows=%v,want %v", test.offset, actual, test.want)
				}
			}
		})
	}
}
