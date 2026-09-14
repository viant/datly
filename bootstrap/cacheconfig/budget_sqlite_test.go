package cacheconfig

import (
	"context"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"testing"
)

func TestBoundedIndexWarmupSQLite(t *testing.T) {
	type row struct {
		GroupID int `sqlx:"group_id"`
		ID      int `sqlx:"id"`
	}
	for _, tc := range []struct {
		name                 string
		group, offset, limit int
		miss                 bool
	}{{"cached_prefix", 1, 0, 1, false}, {"cached_offset", 1, 1, 1, false}, {"offset_boundary", 1, 2, 1, true}, {"incomplete_group", 1, 0, 3, true}, {"unbounded_group", 1, 0, 0, true}, {"unknown_group", 2, 0, 1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(group_id INTEGER,id INTEGER)", "INSERT INTO records VALUES(1,1),(1,2),(1,3),(2,4)"); err != nil {
				t.Fatal(err)
			}
			service, err := (Config{Identity: "bounded", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
			if err != nil {
				t.Fatal(err)
			}
			const baseSQL = "SELECT group_id,id FROM records ORDER BY id"
			if _, err := service.IndexBy(ctx, db.DB, "group_id", baseSQL+" LIMIT 2", nil, &cache.ParmetrizedQuery{IdentitySQL: baseSQL}); err != nil {
				t.Fatal(err)
			}
			if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			reader, err := sqlxread.New(ctx, db.DB, "SELECT group_id,id FROM records WHERE group_id=? ORDER BY id", func() any { return &row{} }, sqlxread.WithCache(service), sqlxread.WithInMatcher(&cache.ParmetrizedQuery{IdentitySQL: baseSQL, By: "group_id", In: []any{tc.group}, Offset: tc.offset, Limit: tc.limit}))
			if err != nil {
				t.Fatal(err)
			}
			actual := []row{}
			err = reader.QueryAll(ctx, func(value any) error { actual = append(actual, *value.(*row)); return nil }, tc.group)
			if tc.miss {
				if err == nil {
					t.Fatalf("incomplete index falsely served rows %v", actual)
				}
				return
			}
			if err != nil || len(actual) != 1 || actual[0].ID != 1+tc.offset {
				t.Fatalf("rows=%v,error=%v", actual, err)
			}
		})
	}
}
