package reader

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/predicatereplay"
	"github.com/viant/datly/internal/readerlookup"
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
			component.Parameters = append(component.Parameters, emptyLookupOptIn())
			if tc.composite {
				view.Columns = append(view.Columns, &data.Column{Name: "Account", Column: "account_id", Tag: `sqlx:"account_id,primaryKey=true"`})
			}
			if tc.complete {
				component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Account", Source: spec.BindSource{Kind: "path", Name: "account"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"r", "account_id"}}}})
			}
			plan, err := compileEmptyLookup(Config{Component: component, InputType: reflect.TypeFor[input](), Plan: &Plan{Root: &ViewPlan{View: view}}})
			if tc.want == 0 && tc.name != "source empty window" && tc.name != "source page" {
				require.ErrorContains(t, err, "complete proven root identity")
				return
			}
			require.NoError(t, err)
			count := 0
			if plan != nil {
				count = len(plan.routes)
			}
			require.Equal(t, tc.want, count)
			if tc.complete {
				require.Len(t, plan.routes[spec.RouteRef{Method: "GET", Path: "/records/{account}/{key}"}].identities, 2)
			}
		})
	}
}

func TestEmptyLookupRejectsUnprovenDynamicSourceAtRegistration(t *testing.T) {
	type input struct{ Key int }
	component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/records/{key}"}}, Parameters: []*spec.Parameter{{Name: "Key", Source: spec.BindSource{Kind: "path", Name: "key"}, Predicates: []*spec.Predicate{{Name: "equal", Args: []string{"r", "id"}}, {Name: "equal", Auth: true, Args: []string{"r", "tenant"}}}}}}
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: `SELECT r.id FROM $Unsafe.Table r`}}, Columns: []*data.Column{{Name: "ID", Column: "id", Tag: `sqlx:"id,primaryKey=true"`}}}
	config := Config{Component: component, InputType: reflect.TypeFor[input](), Plan: &Plan{Root: &ViewPlan{View: view}}}
	component.Parameters = append(component.Parameters, emptyLookupOptIn())
	_, err := compileEmptyLookup(config)
	require.ErrorContains(t, err, "dynamic SQL structure")
	// A collection cannot become an identity read just because it shares marked
	// child metadata. Its existing source behavior remains owned by its reader.
	component.Routes = []*spec.Route{{Method: "GET", Path: "/records"}}
	plan, err := compileEmptyLookup(config)
	require.ErrorContains(t, err, "path-bound GET identity")
	require.Nil(t, plan)
}

func TestEmptyLookupGeneratedPrimaryKeyUsesSQLSource(t *testing.T) {
	type input struct{ Id []int }
	component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/records/{id}"}}, Parameters: []*spec.Parameter{{Name: "Id", Source: spec.BindSource{Kind: "path", Name: "id"}, Predicates: []*spec.Predicate{{Name: "in", Args: []string{"a", "ID"}}}}, {Name: "Auth", Predicates: []*spec.Predicate{{Name: "handler", Auth: true}}}}}
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: "SELECT records.* FROM (SELECT a.* FROM records a) records"}, Columns: []*spec.Column{{Name: "Id", Source: "ID", PrimaryKey: true}}}, Columns: []*data.Column{{Name: "Id", Column: "ID", Tag: `sqlx:"ID,primaryKey=true"`}}}
	component.Parameters = append(component.Parameters, emptyLookupOptIn())
	plan, err := compileEmptyLookup(Config{Component: component, InputType: reflect.TypeFor[input](), Plan: &Plan{Root: &ViewPlan{View: view}}})
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Len(t, plan.routes[spec.RouteRef{Method: "GET", Path: "/records/{id}"}].identities, 1)
}

func emptyLookupOptIn() *spec.Parameter {
	return &spec.Parameter{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}, Tag: `forbiddenNotFoundCheck:"true"`}
}

func TestEmptyLookupExplicitOptIn(t *testing.T) {
	for _, value := range []string{"", `forbiddenNotFoundCheck:"false"`, `forbiddenNotFoundCheck:"invalid"`, `forbiddenNotFoundCheck:"true"`} {
		t.Run(value, func(t *testing.T) {
			parameter := emptyLookupOptIn()
			parameter.Tag = value
			config := Config{Component: &spec.Component{Parameters: []*spec.Parameter{parameter}}, Plan: &Plan{}}
			plan, err := compileEmptyLookup(config)
			require.Nil(t, plan)
			switch value {
			case "", `forbiddenNotFoundCheck:"false"`:
				require.NoError(t, err)
			case `forbiddenNotFoundCheck:"invalid"`:
				require.ErrorContains(t, err, "must be true or false")
			default:
				require.ErrorContains(t, err, "marked authorization predicate")
			}
		})
	}
}

func TestEmptyLookupRepeatedKeyPredicatesAndDistinctPathInputs(t *testing.T) {
	type input struct{ Left, Right []int }
	const path = "/records/{left}/{right}"
	left := &spec.Predicate{Name: "in", Args: []string{"r", "id"}}
	alternate := &spec.Predicate{Name: "in", Group: 3, Args: []string{"r", "id"}}
	right := &spec.Predicate{Name: "in", Args: []string{"r", "id"}}
	component := &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: path}}, Parameters: []*spec.Parameter{
		{Name: "Left", Source: spec.BindSource{Kind: "path", Name: "left"}, Predicates: []*spec.Predicate{left, alternate}},
		{Name: "Right", Source: spec.BindSource{Kind: "path", Name: "right"}, Predicates: []*spec.Predicate{right}},
		{Name: "Auth", Predicates: []*spec.Predicate{{Name: "equal", Auth: true, Args: []string{"r", "tenant"}}}},
		emptyLookupOptIn(),
	}}
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{SQL: "SELECT r.id FROM records r"}}, Columns: []*data.Column{{Name: "ID", Column: "id", Tag: `sqlx:"id,primaryKey=true"`}}}
	root := &ViewPlan{View: view}
	plan, err := compileEmptyLookup(Config{Component: component, InputType: reflect.TypeFor[input](), Plan: &Plan{Root: root}})
	require.NoError(t, err)
	ref := spec.RouteRef{Method: "GET", Path: path}
	ctx := readerlookup.WithRoute(t.Context(), ref)
	require.Len(t, plan.routes[ref].predicates, 3)
	require.False(t, plan.eligible(ctx, reflect.ValueOf(&input{Right: []int{5}}), nil, root))
	require.True(t, plan.eligible(ctx, reflect.ValueOf(&input{Left: []int{4}, Right: []int{5}}), nil, root))
	for _, tc := range []struct {
		name   string
		active []*spec.Predicate
		proven bool
	}{
		{"left missing", []*spec.Predicate{right}, false},
		{"right missing", []*spec.Predicate{left, alternate}, false},
		{"only first left occurrence rendered", []*spec.Predicate{left, right}, true},
		{"only alternate left occurrence rendered", []*spec.Predicate{alternate, right}, true},
		{"all rendered", []*spec.Predicate{left, alternate, right}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture, state := predicatereplay.Capture(ctx)
			for _, p := range tc.active {
				predicatereplay.Record(capture, p, p, "r.id IN (?)", []any{5})
			}
			require.Equal(t, tc.proven, plan.usedIdentity(ctx, state))
		})
	}
}
