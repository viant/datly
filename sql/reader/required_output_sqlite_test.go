package reader_test

import (
	"context"
	"github.com/viant/datly/bootstrap/cacheconfig"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/io/read/cache"
	"reflect"
	"strings"
	"testing"
)

type requiredOutputText string

type requiredOutputRow struct {
	ID       int                `sqlx:"id"`
	Value    requiredOutputText `sqlx:"value"`
	Ordinary *string            `sqlx:"ordinary"`
}
type requiredOutput struct{ Rows []requiredOutputRow }

func TestAllowNullsScalarOutputSQLite(t *testing.T) {
	allow, fallback := true, false
	for _, tc := range []struct {
		name, value, query string
		fails              bool
		allowNulls         *bool
	}{
		{"default_null", "NULL", "SELECT id,value,ordinary FROM records", false, nil},
		{"fallback_null", "NULL", "SELECT id,value,ordinary FROM records", false, &fallback},
		{"null", "NULL", "SELECT id,value,ordinary FROM records", true, &allow},
		{"empty", "''", "SELECT id,value,ordinary FROM records", false, &allow},
		{"json_null", "'null'", "SELECT id,value,ordinary FROM records", false, &allow},
		{"unselected", "NULL", "SELECT id,ordinary FROM records", false, &allow},
		{"absent", "NULL", "SELECT id,value,ordinary FROM records WHERE 1=0", false, &allow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := sqlite.New(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,value TEXT,ordinary TEXT)", "INSERT INTO records VALUES(1,"+tc.value+",NULL)"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Name: "Records", RootView: &spec.View{Name: "records", AllowNulls: tc.allowNulls, Source: &spec.ViewSource{SQL: tc.query}}}
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[requiredOutput](), DirectViewField: "Rows"})
			if err != nil {
				t.Fatal(err)
			}
			store, err := (cacheconfig.Config{Identity: "required/output", Settings: &spec.CacheSettings{Enabled: true, Location: t.TempDir(), TTL: "1m"}}).New()
			if err != nil {
				t.Fatal(err)
			}
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[requiredOutput](), Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}, ReadCaches: map[*data.View]cache.Cache{plan.Root.View: store}})
			if err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 2; pass++ {
				out, err := execution.Read(ctx, &struct{}{}, nil, nil)
				if tc.fails {
					if err == nil || !strings.Contains(err.Error(), "NULL") {
						t.Fatalf("pass %d error=%v", pass, err)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				rows := out.(*requiredOutput).Rows
				if tc.name != "absent" && (len(rows) != 1 || rows[0].Ordinary != nil) {
					t.Fatalf("rows=%#v", rows)
				}
				if tc.name == "json_null" && rows[0].Value != "null" {
					t.Fatalf("rows=%#v", rows)
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
