package reader

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestEmptyLookupAdmissionUsesRootKeyLineage(t *testing.T) {
	type input struct {
		Key     []int
		Account int
		Tenant  int
	}
	for _, tc := range []struct {
		name, sql, alias, column string
		composite, complete      bool
		want                     int
	}{
		{"bound nested value", `SELECT r.id, EXISTS(SELECT 1 FROM watches w WHERE w.id=r.id AND w.contact=$Jwt.UserID) watching FROM records r`, "r", "id", false, false, 1},
		{"scalar subquery window", `SELECT r.id, (SELECT w.contact FROM watches w WHERE w.id=r.id LIMIT 1) watching FROM records r`, "r", "id", false, false, 1},
		{"root key", "SELECT r.id FROM records r", "r", "id", false, false, 1},
		{"nested alias", "SELECT records.* FROM (SELECT a.id FROM records a) records", "a", "id", false, false, 1},
		{"parent path is not root key", "SELECT r.id FROM records r JOIN accounts a ON r.account_id=a.id", "a", "id", false, false, 0},
		{"ordinary filter path", "SELECT r.id,r.account_id FROM records r", "r", "account_id", false, false, 0},
		{"incomplete composite", "SELECT r.id,r.account_id FROM records r", "r", "id", true, false, 0},
		{"complete composite", "SELECT r.id,r.account_id FROM records r", "r", "id", true, true, 1},
		{"source first window", "SELECT r.id FROM records r LIMIT 1", "r", "id", false, false, 1},
		{"source empty window", "SELECT r.id FROM records r LIMIT 0", "r", "id", false, false, 0},
		{"source page", "SELECT r.id FROM records r LIMIT 2 OFFSET 4", "r", "id", false, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/records/{account}/{key}"}, {Method: "GET", Path: "/records"}}, Parameters: []*spec.Parameter{
				{Name: "Key", Source: spec.BindSource{Kind: "path", Name: "key"}, Predicates: []*spec.Predicate{{Name: "in", Args: []string{tc.alias, tc.column}}}},
				{Name: "Tenant", Predicates: []*spec.Predicate{{Name: "equal", Auth: true, Args: []string{"r", "tenant"}}}},
			}}
			view := &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: tc.sql}}, Columns: []*data.Column{{Name: "ID", Column: "id", Tag: `sqlx:"id,primaryKey=true"`}}}
			if tc.composite {
				view.Columns = append(view.Columns, &data.Column{Name: "Account", Column: "account_id", Tag: `sqlx:"account_id,primaryKey=true"`})
			}
			if tc.complete {
				component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Account", Source: spec.BindSource{Kind: "path", Name: "account"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"r", "account_id"}}}})
			}
			plan, err := compileEmptyLookup(Config{Component: component, InputType: reflect.TypeFor[input](), Plan: &Plan{Root: &ViewPlan{View: view}}})
			require.NoError(t, err)
			count := 0
			if plan != nil {
				count = len(plan.routes)
			}
			require.Equal(t, tc.want, count)
			if tc.complete {
				require.Len(t, plan.routes[spec.RouteRef{Method: "GET", Path: "/records/{account}/{key}"}], 2)
			}
		})
	}
}

func TestEmptyLookupRejectsUnprovenDynamicSourceAtRegistration(t *testing.T) {
	type input struct{ Key int }
	component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/records/{key}"}}, Parameters: []*spec.Parameter{{Name: "Key", Source: spec.BindSource{Kind: "path", Name: "key"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"r", "id"}}, {Name: "equal", Auth: true, Args: []string{"r", "tenant"}}}}}}
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: `SELECT r.id FROM $Unsafe.Table r`}}, Columns: []*data.Column{{Name: "ID", Column: "id", Tag: `sqlx:"id,primaryKey=true"`}}}
	config := Config{Component: component, InputType: reflect.TypeFor[input](), Plan: &Plan{Root: &ViewPlan{View: view}}}
	_, err := compileEmptyLookup(config)
	require.ErrorContains(t, err, "dynamic SQL structure")
	// A collection cannot become an identity read just because it shares marked
	// child metadata. Its existing source behavior remains owned by its reader.
	component.Routes = []*spec.Route{{Method: "GET", Path: "/records"}}
	plan, err := compileEmptyLookup(config)
	require.NoError(t, err)
	require.Nil(t, plan)
}
