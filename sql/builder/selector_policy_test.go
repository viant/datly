package builder

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	xstate "github.com/viant/xdatly/state"
)

func TestBuilder_SelectorPolicy(t *testing.T) {
	type input struct{}
	tests := []struct {
		name       string
		policy     *spec.Selector
		selector   *xstate.Selector
		projection []string
		wantSQL    string
		wantErr    string
	}{
		{
			name: "defaults",
			policy: &spec.Selector{
				DefaultOrder: "name ASC",
				DefaultLimit: 2,
			},
			wantSQL: "SELECT id, name FROM users ORDER BY name ASC LIMIT 2",
		},
		{
			name: "request limit is capped by original style default maximum",
			policy: &spec.Selector{
				AllowLimit:   true,
				DefaultLimit: 10,
			},
			selector: &xstate.Selector{Limit: 25},
			wantSQL:  "SELECT id, name FROM users LIMIT 10",
		},
		{
			name: "page uses effective limit",
			policy: &spec.Selector{
				AllowLimit:   true,
				AllowPage:    true,
				DefaultLimit: 5,
			},
			selector: &xstate.Selector{Page: 3},
			wantSQL:  "SELECT id, name FROM users LIMIT 5 OFFSET 10",
		},
		{
			name: "order alias and colon direction",
			policy: &spec.Selector{
				AllowOrderBy: true,
				Orderable:    []spec.FieldPath{"name"},
				OrderAliases: map[string]spec.FieldPath{"display": "name"},
			},
			selector: &xstate.Selector{OrderBy: "display:desc"},
			wantSQL:  "SELECT id, name FROM users ORDER BY name DESC",
		},
		{
			name:     "numeric order position",
			policy:   &spec.Selector{AllowOrderBy: true},
			selector: &xstate.Selector{OrderBy: "2 DESC"},
			wantSQL:  "SELECT id, name FROM users ORDER BY 2 DESC",
		},
		{
			name:       "projection allowed",
			policy:     &spec.Selector{AllowFields: true},
			projection: []string{"name"},
			wantSQL:    "SELECT name FROM users",
		},
		{
			name:       "projection denied",
			policy:     &spec.Selector{},
			projection: []string{"name"},
			wantErr:    "projection is not allowed",
		},
		{
			name:     "order denied",
			policy:   &spec.Selector{},
			selector: &xstate.Selector{OrderBy: "name"},
			wantErr:  "order by is not allowed",
		},
		{
			name:     "order field denied",
			policy:   &spec.Selector{AllowOrderBy: true, Orderable: []spec.FieldPath{"id"}},
			selector: &xstate.Selector{OrderBy: "name"},
			wantErr:  "field \"name\" is not allowed",
		},
		{
			name:     "order expression denied",
			policy:   &spec.Selector{AllowOrderBy: true},
			selector: &xstate.Selector{OrderBy: "LOWER(name)"},
			wantErr:  "only supports output names or numeric positions",
		},
		{
			name:     "criteria denied",
			policy:   &spec.Selector{},
			selector: &xstate.Selector{Criteria: "id > ?", Placeholders: []interface{}{1}},
			wantErr:  "criteria is not allowed",
		},
		{
			name:     "limit denied",
			policy:   &spec.Selector{},
			selector: &xstate.Selector{Limit: 1},
			wantErr:  "limit is not allowed",
		},
		{
			name:     "offset without limit",
			policy:   &spec.Selector{AllowOffset: true},
			selector: &xstate.Selector{Offset: 1},
			wantErr:  "offset requires a positive limit",
		},
		{
			name:     "page denied",
			policy:   &spec.Selector{DefaultLimit: 5},
			selector: &xstate.Selector{Page: 2},
			wantErr:  "page is not allowed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := NewBuilder().Build(context.Background(),
				WithBuilderSQL("SELECT id, name FROM users"),
				WithBuilderInput(reflect.ValueOf(input{})),
				WithBuilderSelectorPolicy(test.policy),
				WithBuilderSelector(test.selector),
				WithBuilderProjection(test.projection),
			)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("expected error containing %q, got %v", test.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("build failed: %v", err)
			}
			if query.SQL != test.wantSQL {
				t.Fatalf("unexpected SQL\nwant: %s\n got: %s", test.wantSQL, query.SQL)
			}
		})
	}
}

func TestBuilder_SelectorPolicyDerivedFromComponent(t *testing.T) {
	component := &spec.Component{RootView: &spec.View{
		Selector: &spec.Selector{DefaultLimit: 3},
	}}
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderComponent(component),
		WithBuilderSQL("SELECT id FROM users"),
		WithBuilderInput(reflect.ValueOf(struct{}{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if query.SQL != "SELECT id FROM users LIMIT 3" {
		t.Fatalf("unexpected SQL: %s", query.SQL)
	}
}

func TestBuilder_CacheSQLPreservesMatcherWindow(t *testing.T) {
	staticLimit, staticOffset := 2, 3
	overrideLimit, overrideOffset := 50, 5
	testCases := []struct {
		name       string
		controls   *spec.ViewControls
		policy     *spec.Selector
		selector   *xstate.Selector
		wantLimit  int
		wantOffset int
	}{
		{
			name:       "selector limit and offset",
			policy:     &spec.Selector{AllowLimit: true, AllowOffset: true, DefaultLimit: 100},
			selector:   &xstate.Selector{Limit: 10, Offset: 20},
			wantLimit:  10,
			wantOffset: 20,
		},
		{
			name:       "static controls",
			controls:   &spec.ViewControls{Limit: &staticLimit, Offset: &staticOffset},
			wantLimit:  2,
			wantOffset: 3,
		},
		{
			name:      "policy default",
			policy:    &spec.Selector{DefaultLimit: 100},
			wantLimit: 100,
		},
		{
			name:      "selector limit is clamped to default",
			policy:    &spec.Selector{AllowLimit: true, DefaultLimit: 100},
			selector:  &xstate.Selector{Limit: 250},
			wantLimit: 100,
		},
		{
			name:       "page derives offset from effective limit",
			policy:     &spec.Selector{AllowPage: true, DefaultLimit: 10},
			selector:   &xstate.Selector{Page: 3},
			wantLimit:  10,
			wantOffset: 20,
		},
		{
			name:   "no limit policy suppresses default",
			policy: &spec.Selector{DefaultLimit: 100, NoLimit: true},
		},
		{
			name:      "no limit policy permits explicit limit without default clamp",
			policy:    &spec.Selector{AllowLimit: true, DefaultLimit: 100, NoLimit: true},
			selector:  &xstate.Selector{Limit: 250},
			wantLimit: 250,
		},
		{
			name:       "selector overrides static controls",
			controls:   &spec.ViewControls{Limit: &overrideLimit, Offset: &overrideOffset},
			policy:     &spec.Selector{AllowLimit: true, AllowOffset: true},
			selector:   &xstate.Selector{Limit: 10, Offset: 20},
			wantLimit:  10,
			wantOffset: 20,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			query, err := NewBuilder().CacheSQL(context.Background(),
				WithBuilderSQL("SELECT id, name FROM users"),
				WithBuilderControls(testCase.controls),
				WithBuilderInput(reflect.ValueOf(struct{}{})),
				WithBuilderSelectorPolicy(testCase.policy),
				WithBuilderSelector(testCase.selector),
				WithBuilderMatcher("id", []any{7, 8}),
			)
			if err != nil {
				t.Fatalf("cache build failed: %v", err)
			}
			if query.SQL != "SELECT id, name FROM users" {
				t.Fatalf("expected cache SQL without pagination, got %s", query.SQL)
			}
			if query.Offset != testCase.wantOffset || query.Limit != testCase.wantLimit {
				t.Fatalf("expected matcher window offset=%d limit=%d, got offset=%d limit=%d", testCase.wantOffset, testCase.wantLimit, query.Offset, query.Limit)
			}
			if query.By != "id" || !reflect.DeepEqual(query.In, []interface{}{7, 8}) {
				t.Fatalf("unexpected matcher identity: by=%q in=%v", query.By, query.In)
			}
		})
	}
}

func TestBuilder_SelectorPolicySQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);`,
		`INSERT INTO users(id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderSQL("SELECT id, name FROM users $WHERE_SELECTOR_CRITERIA"),
		WithBuilderInput(reflect.ValueOf(struct{}{})),
		WithBuilderSelectorPolicy(&spec.Selector{
			AllowCriteria: true,
			Filterable:    []spec.FieldPath{"id"},
			AllowOrderBy:  true,
			AllowLimit:    true,
			AllowPage:     true,
			DefaultLimit:  1,
			Orderable:     []spec.FieldPath{"id"},
		}),
		WithBuilderSelector(&xstate.Selector{
			Criteria:     "id > ?",
			Placeholders: []interface{}{1},
			OrderBy:      "id DESC",
			Page:         2,
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	rows, err := h.DB.QueryContext(context.Background(), query.SQL, query.Args...)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected one row")
	}
	var id int
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if id != 2 || name != "b" {
		t.Fatalf("expected second descending filtered row (2,b), got (%d,%s)", id, name)
	}
	if rows.Next() {
		t.Fatalf("expected default limit to return one row")
	}
}

func TestBuilder_SelectorCriteriaArgumentOrderSQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE grouped_users (tenant_id INTEGER);`,
		`INSERT INTO grouped_users(tenant_id) VALUES (1), (1), (1), (2), (2), (2), (3), (3);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{ Min int }
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderSQL("SELECT tenant_id, COUNT(*) AS total FROM grouped_users GROUP BY tenant_id HAVING COUNT(*) > :Min ORDER BY tenant_id"),
		WithBuilderInput(reflect.ValueOf(input{Min: 2})),
		withTestParameterProjection(t, reflect.TypeOf(input{}), map[string]string{"Min": "Min"}),
		WithBuilderSelectorPolicy(&spec.Selector{AllowCriteria: true, Filterable: []spec.FieldPath{"tenant_id"}}),
		WithBuilderSelector(&xstate.Selector{Criteria: "tenant_id > ?", Placeholders: []any{1}}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	rows, err := h.DB.QueryContext(context.Background(), query.SQL, query.Args...)
	if err != nil {
		t.Fatalf("query failed: %v; SQL=%s args=%v", err, query.SQL, query.Args)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected grouped tenant row")
	}
	var tenantID, total int
	if err := rows.Scan(&tenantID, &total); err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if tenantID != 2 || total != 3 {
		t.Fatalf("expected tenant 2 total 3, got tenant %d total %d", tenantID, total)
	}
	if rows.Next() {
		t.Fatal("expected exactly one grouped tenant row")
	}
}
