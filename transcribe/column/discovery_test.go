package column

import (
	"context"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestRefinerPseudoColumnMappingSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := h.ExecStatements(ctx, `CREATE TABLE records (id INTEGER, unknown)`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, projection, failure string
		column                    *spec.Column
	}{
		{"explicit transient", `id, '' AS bounds`, "", &spec.Column{Name: "bounds", Type: spec.TypeRef{Name: "int"}, ExplicitType: true, Tag: `sqlx:"-"`}},
		{"canonical alias", `id, '' AS bounds`, "", &spec.Column{Name: "Logical", Source: "bounds", Type: spec.TypeRef{Name: "int"}, ExplicitType: true, Tag: `sqlx:"-"`}},
		{"inferred name with source", `id, '' AS bounds`, "", &spec.Column{Name: "Logical", NameInferred: true, Source: "bounds", Type: spec.TypeRef{Name: "int"}, ExplicitType: true, Tag: `sqlx:"-"`}},
		{"inferred name is not alias", `id, '' AS Logical`, "Logical", &spec.Column{Name: "Logical", NameInferred: true, Source: "bounds", Type: spec.TypeRef{Name: "int"}, ExplicitType: true, Tag: `sqlx:"-"`}},
		{"cast alone", `id, '' AS bounds`, "", &spec.Column{Name: "bounds", Type: spec.TypeRef{Name: "int"}, ExplicitType: true}},
		{"tag alone", `id, '' AS bounds`, "", &spec.Column{Name: "bounds", Tag: `sqlx:"-"`}},
		{"inferred type", `id, '' AS bounds`, "", &spec.Column{Name: "bounds", Type: spec.TypeRef{Name: "int"}, Tag: `sqlx:"-"`}},
		{"missing explicit type", `id, '' AS bounds`, "bounds", &spec.Column{Name: "bounds", ExplicitType: true, Tag: `sqlx:"-"`}},
		{"unrelated unknown physical", `id, '' AS bounds, unknown`, "unknown", &spec.Column{Name: "bounds", Type: spec.TypeRef{Name: "int"}, ExplicitType: true, Tag: `sqlx:"-"`}},
		{"missing physical", `id, missing AS bounds`, "missing", &spec.Column{Name: "bounds", Type: spec.TypeRef{Name: "int"}, ExplicitType: true, Tag: `sqlx:"-"`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			SQL := "SELECT r.* FROM (SELECT " + tc.projection + " FROM records) r"
			view := &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: SQL}, Columns: []*spec.Column{tc.column}}
			component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: view}
			err := New(Connections{"main": h.DB}).Refine(ctx, component, nil, nil)
			if tc.failure != "" {
				if err == nil || !strings.Contains(err.Error(), tc.failure) {
					t.Fatalf("expected failure for %s, got %v", tc.failure, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			wantType := tc.column.Type
			if wantType.IsZero() {
				wantType.Name = "string"
			}
			if len(view.Columns) != 2 || view.Columns[0].Name != tc.column.Name || view.Columns[0].Type != wantType || view.Columns[0].ExplicitType != tc.column.ExplicitType || view.Columns[0].Tag != tc.column.Tag || view.Columns[0].DatabaseType != "" || view.Columns[1].Type.Name != "int" {
				t.Fatalf("metadata lost: %+v", view.Columns)
			}
			if view.Source.SQL != SQL || tc.column.DatabaseType != "" || tc.column.Nullable {
				t.Fatal("discovery mutated authored SQL or input column")
			}
		})
	}
}
