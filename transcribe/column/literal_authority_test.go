package column

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestLiteralPlaceholderAuthoritySQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE records(id INTEGER, bounds)`, `CREATE TABLE known(id INTEGER)`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, sql, want string
		cast            bool
	}{
		{"mapped physical CAST", `SELECT id,bounds FROM records`, "int", true},
		{"physical explicit", `SELECT id,bounds FROM records`, "int", true},
		{"physical named", `SELECT r.* FROM (SELECT id,bounds FROM records) r`, "int", true},
		{"physical wildcard", `SELECT r.* FROM records r`, "int", true},
		{"undeclared physical", `SELECT id,bounds FROM records`, "reject", false},
		{"opaque CTE CAST", `SELECT r.* FROM (WITH c AS (SELECT id,coalesce(bounds,7) AS computed FROM records) SELECT id, computed+0 AS bounds FROM c) r`, "int", true},
		{"computed CAST", `SELECT r.* FROM (SELECT id, CASE WHEN id>0 THEN 3 ELSE 0 END AS bounds FROM known) r`, "int", true},
		{"undeclared computed", `SELECT r.* FROM (SELECT id, id+0 AS bounds FROM known) r`, "reject", false},
		{"missing CAST output", `SELECT id FROM known`, "reject", true},
		{"missing opaque wildcard CAST output", `SELECT r.* FROM (WITH RECURSIVE keys(id) AS (VALUES(1)) SELECT id FROM keys) r`, "reject", true},
		{"missing opaque direct CAST output", `WITH RECURSIVE keys(id) AS (VALUES(1)) SELECT id FROM keys`, "reject", true},
		{"duplicate result labels", `SELECT '' AS bounds, 0 AS bounds FROM known`, "reject", true},
		{"quoted string", `SELECT r.* FROM (SELECT id,'' AS "bounds" FROM known) r`, "string", false},
		{"backtick integer", "SELECT r.* FROM (SELECT id,0 AS `bounds` FROM known) r", "int", false},
		{"string default", `SELECT r.* FROM (SELECT id,'' AS bounds FROM known) r`, "string", false},
		{"int default", `SELECT r.* FROM (SELECT id,0 AS bounds FROM known) r`, "int", false},
		{"string cast", `SELECT r.* FROM (SELECT id,'' AS bounds FROM known) r`, "int", true},
		{"wildcard string", `SELECT r.* FROM (SELECT o.*, '' AS bounds FROM known o) r`, "string", false},
		{"wildcard collision", `SELECT r.* FROM (SELECT o.*, '' AS bounds FROM records o) r`, "reject", true},
		{"duplicate explicit", `SELECT r.* FROM (SELECT bounds,'' AS bounds FROM records) r`, "reject", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: tc.sql}}
			if tc.cast {
				view.Columns = []*spec.Column{{Name: "bounds", Source: "bounds", ExplicitType: true, Type: spec.TypeRef{Name: "int"}, Tag: `sqlx:"-"`}}
			}
			if strings.HasPrefix(tc.name, "missing opaque") {
				view.Columns = append(view.Columns, &spec.Column{Name: "id", Source: "id", ExplicitType: true, Type: spec.TypeRef{Name: "int"}})
			}
			if tc.name == "mapped physical CAST" {
				view.Columns[0].Tag = ""
			}
			err := New(Connections{"main": h.DB}).Refine(ctx, &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}, nil, nil)
			if tc.want == "reject" {
				if err == nil {
					t.Fatal("undeclared, missing or duplicate output accepted")
				}
				if strings.HasPrefix(tc.name, "missing") {
					if !strings.Contains(err.Error(), "bounds is absent from the SQL projection") {
						t.Fatalf("expected missing result label, got %v", err)
					}
					t.Logf("missing output rejected: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, column := range view.Columns {
				if strings.EqualFold(column.Name, "bounds") {
					if got := column.EffectiveType(); got.Name != tc.want || got.Pointer {
						t.Fatalf("default/CAST type: %+v", got)
					}
					return
				}
			}
			t.Fatal("bounds missing")
		})
	}
}

func TestOpaqueCTECASTPreservesSourceSQLite(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	const inner = `WITH RECURSIVE numbers(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM numbers WHERE n<2) SELECT sum(n) AS value FROM numbers`
	SQL := "SELECT r.* FROM (" + inner + ") r"
	view := &spec.View{Name: "Numbers", Source: &spec.ViewSource{SQL: SQL}, Columns: []*spec.Column{{Name: "value", Source: "value", Type: spec.TypeRef{Name: "int"}, ExplicitType: true}}}
	err := New(Connections{"main": h.DB}).Refine(ctx, &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(view.Source.SQL, inner) {
		t.Fatal("authored inner SQL changed")
	}
	discovery, err := discoveryQuery(view.Source)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(discovery, inner) {
		t.Fatalf("inner CTE changed: %s", discovery)
	}
}

func TestOpaqueDirectCTECASTDiscoverySQLite(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	const SQL = `WITH RECURSIVE numbers(n) AS (VALUES(0) UNION ALL SELECT n+1 FROM numbers WHERE n<2) SELECT sum(n) AS value FROM numbers`
	view := &spec.View{Name: "Numbers", Source: &spec.ViewSource{SQL: SQL}, Columns: []*spec.Column{{Name: "value", Source: "value", Type: spec.TypeRef{Name: "int"}, ExplicitType: true}}}
	if err := New(Connections{"main": h.DB}).Refine(ctx, &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if view.Source.SQL != SQL {
		t.Fatal("inner SQL changed")
	}
}
