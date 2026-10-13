package reader_test

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/io/read/cache"
)

type computedTimestampRow struct {
	ID          int        `sqlx:"id"`
	CompletedAt *time.Time `sqlx:"completed_at"`
}
type computedTimestampOutput struct{ Rows []computedTimestampRow }

func TestComputedNullableTimestampSQLite(t *testing.T) {
	for _, tc := range []struct {
		name, insert, query string
		malformed           bool
	}{
		{"nullable_and_populated", "INSERT INTO records VALUES(1,'pending',NULL),(2,'done','2026-10-12 08:59:41.123456789'),(3,'pending','2026-10-12 09:00:00')", "SELECT id, CASE WHEN status='done' THEN completed_at ELSE NULL END AS completed_at FROM records ORDER BY id", false},
		{"date_only", "INSERT INTO records VALUES(1,'pending',NULL),(2,'done','2026-10-12'),(3,'pending','2026-10-13')", "SELECT id, CASE WHEN status='done' THEN completed_at ELSE NULL END AS completed_at FROM records ORDER BY id", false},
		{"physical_datetime", "INSERT INTO records VALUES(1,'pending',NULL),(2,'done','2026-10-12 08:59:41.123456789'),(3,'pending','2026-10-12 09:00:00')", "SELECT id, completed_at FROM records ORDER BY id", false},
		{"malformed", "INSERT INTO records VALUES(1,'done','not-a-timestamp')", "SELECT id, CASE WHEN status='done' THEN completed_at ELSE NULL END AS completed_at FROM records ORDER BY id", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,status TEXT,completed_at DATETIME)", tc.insert); err != nil {
				t.Fatal(err)
			}
			allowNulls := true
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "ComputedTimestamps"}, RootView: &spec.View{Name: "records", AllowNulls: &allowNulls, Source: &spec.ViewSource{SQL: tc.query}}}
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[computedTimestampOutput](), DirectViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			store, err := (cacheconfig.Config{Identity: "computed/timestamp", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
			if err != nil {
				t.Fatal(err)
			}
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[computedTimestampOutput](), Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: map[*data.View]cache.Cache{plan.Root.View: store}})
			if err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 2; pass++ {
				actual, err := execution.Read(ctx, &struct{}{}, nil, nil)
				if tc.malformed {
					if err == nil || !strings.Contains(err.Error(), "invalid timestamp value") {
						t.Fatalf("pass %d err=%v", pass, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				rows := actual.(*computedTimestampOutput).Rows
				if len(rows) != 3 || rows[0].CompletedAt != nil || (tc.name != "physical_datetime" && rows[2].CompletedAt != nil) || rows[1].CompletedAt == nil {
					t.Fatalf("pass %d rows=%#v", pass, rows)
				}
				expected := time.Date(2026, 10, 12, 8, 59, 41, 123456789, time.UTC)
				if tc.name == "date_only" {
					expected = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
				}
				if !rows[1].CompletedAt.Equal(expected) {
					t.Fatalf("pass %d completed=%v", pass, rows[1].CompletedAt)
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
