package cacheconfig

import (
	"context"
	"fmt"
	"github.com/viant/sqlx/io/read/cache/aerospike"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

func TestNativeCacheConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Config)
	}{
		{"missing_settings", func(c *Config) { c.Settings = nil }},
		{"disabled", func(c *Config) { c.Settings.Enabled = false }},
		{"missing_identity", func(c *Config) { c.Identity = "" }},
		{"missing_location", func(c *Config) { c.Settings.Location = "" }},
		{"missing_ttl", func(c *Config) { c.Settings.TTL = "" }},
		{"negative_ttl", func(c *Config) { c.Settings.TTL = "-1s" }},
		{"bad_ttl", func(c *Config) { c.Settings.TTL = "later" }},
		{"conflicting_ttl", func(c *Config) { c.Settings.TimeToLiveMs = 1 }},
		{"negative_milliseconds", func(c *Config) { c.Settings.TimeToLiveMs = -1 }},
		{"unknown_provider", func(c *Config) { c.Settings.Provider = "unknown" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := Config{Identity: "component/root", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}
			tc.change(&config)
			if _, err := config.New(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestNativeIndexedCacheSQLite(t *testing.T) {
	type row struct {
		GroupID int `sqlx:"group_id"`
		ID      int `sqlx:"id"`
	}
	for _, backend := range []string{"afs", "aerospike"} {
		t.Run(backend, func(t *testing.T) {
			provider := "afs"
			if backend == "aerospike" {
				provider = os.Getenv("DATLY_TEST_AEROSPIKE")
				if provider == "" {
					t.Skip("DATLY_TEST_AEROSPIKE is unset")
				}
			}
			var pool aerospike.Pool
			t.Cleanup(func() { _ = pool.Close() })
			for _, tc := range []struct {
				name          string
				in            []any
				offset, limit int
				want          []row
			}{
				{"one_group", []any{1}, 0, 0, []row{{1, 1}, {1, 2}, {1, 3}}},
				{"two_groups", []any{1, 2}, 0, 0, []row{{1, 1}, {1, 2}, {1, 3}, {2, 4}, {2, 5}}},
				{"per_group_pagination", []any{1, 2}, 1, 1, []row{{1, 2}, {2, 5}}},
				{"missing_group", []any{3}, 0, 0, []row{}},
				{"duplicate_group", []any{1, 1}, 0, 1, []row{{1, 1}}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx := context.Background()
					db := sqlite.New(t)
					if err := db.ExecStatements(ctx, "CREATE TABLE records(group_id INTEGER,id INTEGER)", "INSERT INTO records VALUES (1,1),(1,2),(1,3),(2,4),(2,5)"); err != nil {
						t.Fatal(err)
					}
					location := t.TempDir()
					if backend == "aerospike" {
						location = "datly_validation"
					}
					service, err := (Config{Identity: fmt.Sprintf("component/root/%s/%s/%d", location, t.Name(), time.Now().UnixNano()), Aerospike: &pool, Settings: &spec.CacheSettings{Enabled: true, Provider: provider, Location: location, TTL: "30s"}}).New()
					if err != nil {
						t.Fatal(err)
					}
					const baseSQL = "SELECT group_id,id FROM records ORDER BY id"
					wantGroups := 2
					if backend == "aerospike" {
						// Native Aerospike counts its marker.
						wantGroups++
					}
					if groups, err := service.IndexBy(ctx, db.DB, "group_id", baseSQL, nil); err != nil || groups != wantGroups {
						t.Fatalf("IndexBy=%d,%v", groups, err)
					}
					if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
						t.Fatal(err)
					}
					if backend == "afs" {
						if groups, err := service.IndexBy(ctx, db.DB, "group_id", baseSQL, nil); err != nil || groups != 2 {
							t.Fatalf("repeat indexed warmup=%d,%v", groups, err)
						}
					}
					reader, err := sqlxread.New(ctx, db.DB, "SELECT group_id,id FROM records WHERE group_id = ?", func() any { return &row{} }, sqlxread.WithCache(service), sqlxread.WithInMatcher(&cache.ParmetrizedQuery{By: "group_id", IdentitySQL: baseSQL, In: tc.in, Offset: tc.offset, Limit: tc.limit}))
					if err != nil {
						t.Fatal(err)
					}
					actual := []row{}
					if err := reader.QueryAll(ctx, func(value any) error { actual = append(actual, *value.(*row)); return nil }, 1); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(actual, tc.want) {
						t.Fatalf("rows=%v,want %v", actual, tc.want)
					}
					projected, err := sqlxread.New(ctx, db.DB, "SELECT id FROM records WHERE group_id = ?", func() any { return &row{} }, sqlxread.WithCache(service), sqlxread.WithInMatcher(&cache.ParmetrizedQuery{By: "group_id", IdentitySQL: baseSQL, In: tc.in, Offset: tc.offset, Limit: tc.limit, RequestedFields: []cache.ProjectionField{{ColumnName: "id"}}}))
					if err != nil {
						t.Fatal(err)
					}
					actual = []row{}
					if err := projected.QueryAll(ctx, func(value any) error { actual = append(actual, *value.(*row)); return nil }, 1); err != nil {
						t.Fatal(err)
					}
					want := append([]row{}, tc.want...)
					for i := range want {
						want[i].GroupID = 0
					}
					if !reflect.DeepEqual(actual, want) {
						t.Fatalf("projected rows=%v,want %v", actual, want)
					}
				})
			}
		})
	}
}

func TestNativeCacheSQLiteReplay(t *testing.T) {
	type row struct {
		ID   int    `sqlx:"id"`
		Name string `sqlx:"name"`
	}
	for _, tc := range []struct {
		name    string
		warm    bool
		minimum int
		want    []row
	}{
		{"cold_read", false, 1, []row{{1, "one"}, {2, "two"}}},
		{"warmup", true, 1, []row{{1, "one"}, {2, "two"}}},
		{"warmup_empty", true, 3, []row{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, name TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')"); err != nil {
				t.Fatal(err)
			}
			config := Config{Identity: "component/root", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}
			service, err := config.New()
			if err != nil {
				t.Fatal(err)
			}
			const query = "SELECT id, name FROM records WHERE id >= ? ORDER BY id"
			if tc.warm {
				if groups, err := service.IndexBy(ctx, db.DB, "", query, []any{tc.minimum}); err != nil || groups != 1 {
					t.Fatalf("IndexBy = %d, %v", groups, err)
				}
			} else {
				reader, err := sqlxread.New(ctx, db.DB, query, func() any { return &row{} }, sqlxread.WithCache(service))
				if err != nil {
					t.Fatal(err)
				}
				if err := reader.QueryAll(ctx, func(any) error { return nil }, tc.minimum); err != nil {
					t.Fatal(err)
				}
				if reader.Stmt() != nil {
					_ = reader.Stmt().Close()
				}
			}
			if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
				t.Fatal(err)
			}
			if tc.warm {
				if groups, err := service.IndexBy(ctx, db.DB, "", query, []any{tc.minimum}); err != nil || groups != 1 {
					t.Fatalf("repeat warmup=%d,%v", groups, err)
				}
			}
			reader, err := sqlxread.New(ctx, db.DB, query, func() any { return &row{} }, sqlxread.WithCache(service))
			if err != nil {
				t.Fatal(err)
			}
			actual := []row{}
			if err := reader.QueryAll(ctx, func(value any) error { actual = append(actual, *value.(*row)); return nil }, tc.minimum); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, tc.want) {
				t.Fatalf("rows = %+v, want %+v", actual, tc.want)
			}
			// An independently prepared view must never replay this view's entry.
			config.Identity = "component/other-connector"
			other, err := config.New()
			if err != nil {
				t.Fatal(err)
			}
			isolated, err := sqlxread.New(ctx, db.DB, query, func() any { return &row{} }, sqlxread.WithCache(other))
			if err != nil {
				t.Fatal(err)
			}
			if err := isolated.QueryAll(ctx, func(any) error { return nil }, tc.minimum); err == nil {
				t.Fatal("unrelated view replayed another view's cache")
			}
		})
	}
}
