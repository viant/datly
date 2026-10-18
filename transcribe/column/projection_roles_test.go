package column

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func TestRefinerDiscoversGroupedProjectionRolesSQLite(t *testing.T) {
	harness := testharness.NewSQLiteHarness(t)
	ctx := context.Background()
	if err := harness.ExecStatements(ctx, `CREATE TABLE sales(region TEXT,amount INTEGER)`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name      string
		groupable bool
		wrapper   bool
		overrides bool
	}{
		{name: "fresh", groupable: true}, {name: "transparent wrapper", groupable: true, wrapper: true},
		{name: "authored overrides", groupable: true, overrides: true}, {name: "non-groupable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text := "SELECT region, SUM(amount) AS total FROM sales GROUP BY region"
			if tc.wrapper {
				text = "SELECT c.* FROM (" + text + ") c"
			}
			// SQLite empty-result discovery needs aggregate type authority, independently
			// of role inference. This declaration supplies no Groupable default.
			total := &spec.Column{Name: "total", Type: spec.TypeRef{Name: "int64"}, ExplicitType: true}
			columns := []*spec.Column{total}
			if tc.overrides {
				yes := true
				total.Groupable = &yes
				columns = append(columns, &spec.Column{Name: "region", Tag: `groupable:"false"`})
			}
			component := &spec.Component{Settings: &spec.Settings{DefaultConnector: "main"}, RootView: &spec.View{Name: "Sales", Groupable: &tc.groupable, Source: &spec.ViewSource{SQL: text}, Columns: columns}}
			if err := New(Connections{"main": harness.DB}).Refine(ctx, component, nil, nil); err != nil {
				t.Fatal(err)
			}
			actual := component.RootView.Columns
			if len(actual) != 2 {
				t.Fatalf("columns=%+v", actual)
			}
			// Existing authored-first merge order is retained; no role sorting occurs.
			names := []string{actual[0].Name, actual[1].Name}
			if !reflect.DeepEqual(names, []string{"total", "region"}) {
				t.Fatalf("membership/order=%v", names)
			}
			want := []bool{false, tc.groupable}
			if tc.overrides {
				want = []bool{true, false}
			}
			for i, column := range actual {
				if column.Groupable == nil || *column.Groupable != want[i] {
					t.Fatalf("%s role=%v want %v", column.Name, column.Groupable, want[i])
				}
			}
			if component.RootView.Source.SQL != text {
				t.Fatalf("SQL changed: %s", component.RootView.Source.SQL)
			}
		})
	}
}

func TestDiscoveredProjectionRolesKeepOpaqueAndUnmatchedDefaults(t *testing.T) {
	for _, text := range []string{"opaque SQL", "SELECT * FROM sales", "SELECT SUM(amount) AS other FROM sales", "SELECT region AS total, SUM(amount) AS total FROM sales"} {
		yes := true
		column := &spec.Column{Name: "total", Groupable: &yes}
		applyDiscoveredProjectionRoles([]*spec.Column{column}, text)
		if !*column.Groupable {
			t.Fatalf("opaque/unmatched/ambiguous SQL changed default: %s", text)
		}
	}
}

func TestDiscoveredProjectionRolesUseUniqueNormalizedResultNames(t *testing.T) {
	yes := true
	dimension := &spec.Column{Name: "REGION", Groupable: &yes}
	measure := &spec.Column{Name: "TOTAL", Groupable: &yes}
	columns := []*spec.Column{measure, dimension}
	applyDiscoveredProjectionRoles(columns, `SELECT region, SUM(amount) AS total FROM sales GROUP BY region`)
	if *measure.Groupable || !*dimension.Groupable || columns[0] != measure || columns[1] != dimension {
		t.Fatalf("roles or order changed unexpectedly: %+v", columns)
	}
	first, second := &spec.Column{Name: "total", Groupable: &yes}, &spec.Column{Name: "TOTAL", Groupable: &yes}
	applyDiscoveredProjectionRoles([]*spec.Column{first, second}, `SELECT SUM(amount) AS total FROM sales`)
	if !*first.Groupable || !*second.Groupable {
		t.Fatal("ambiguous discovered results changed defaults")
	}
}
