package column

import (
	"context"
	_ "embed"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	sqltemplate "github.com/viant/datly/sql/template"
)

//go:embed testdata/conditional_union.sql
var conditionalUnionSQL string

// This example changes query structure, rather than adding an optional filter.
// Both branches retain the same output contract and bind their tenant values.
func TestConditionalUnionSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx,
		`CREATE TABLE active_records (id INTEGER, name TEXT, tenant_id INTEGER)`,
		`CREATE TABLE archived_records (id INTEGER, name TEXT, tenant_id INTEGER)`,
		`INSERT INTO active_records VALUES (1,'active',7),(2,'duplicate',7),(99,'foreign',8)`,
		`INSERT INTO archived_records VALUES (2,'duplicate',7),(3,'archived',7),(100,'foreign',8)`,
	); err != nil {
		t.Fatal(err)
	}
	type input struct {
		IncludeArchive bool
		TenantIDs      []int
	}
	program, err := (sqltemplate.Compiler{Source: conditionalUnionSQL, InputType: reflect.TypeFor[input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	if program == nil {
		t.Fatal("conditional SQL must retain its template program")
	}
	type row struct {
		ID   int
		Name string
	}
	for _, tc := range []struct {
		name    string
		archive bool
		want    []row
		args    []any
	}{
		{"active_only", false, []row{{1, "active"}, {2, "duplicate"}}, []any{7}},
		{"include_archive", true, []row{{1, "active"}, {2, "duplicate"}, {2, "duplicate"}, {3, "archived"}}, []any{7, 7}},
		{"active_again", false, []row{{1, "active"}, {2, "duplicate"}}, []any{7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound := reflect.ValueOf(input{IncludeArchive: tc.archive, TenantIDs: []int{7}})
			component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: conditionalUnionSQL}}}
			if err := New(Connections{"main": h.DB}).Refine(ctx, component, nil, &TemplateInput{Value: bound}); err != nil {
				t.Fatal(err)
			}
			if component.RootView.Source.SQL != conditionalUnionSQL {
				t.Fatal("discovery rewrote authored conditional SQL")
			}
			cols := component.RootView.Columns
			if len(cols) != 2 || cols[0].Name != "id" || cols[1].Name != "name" {
				t.Fatalf("unexpected output contract: %+v", cols)
			}
			result, err := program.Evaluate(ctx, sqltemplate.Invocation{Input: bound})
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(result.SQL, "#if") || strings.Contains(result.SQL, "UNION ALL") != tc.archive {
				t.Fatalf("wrong branch SQL: %s", result.SQL)
			}
			if !reflect.DeepEqual(result.Args, tc.args) {
				t.Fatalf("bindings: got %v, want %v", result.Args, tc.args)
			}
			records, err := h.DB.QueryContext(ctx, result.SQL, result.Args...)
			if err != nil {
				t.Fatal(err)
			}
			defer records.Close()
			var actual []row
			for records.Next() {
				var item row
				if err := records.Scan(&item.ID, &item.Name); err != nil {
					t.Fatal(err)
				}
				actual = append(actual, item)
			}
			if err := records.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, tc.want) {
				t.Fatalf("rows: got %+v, want %+v", actual, tc.want)
			}
		})
	}
}
