package builder

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/assertly"
	"github.com/viant/datly/data"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/metadata/info"
	xstate "github.com/viant/xdatly/state"
)

func TestBuilder_Build_BindsNamedPlaceholders(t *testing.T) {
	type input struct {
		ID int
	}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name FROM users WHERE id = :ID"),
		WithBuilderInput(reflect.ValueOf(input{ID: 7})),
		withTestParameterProjection(t, reflect.TypeOf(input{}), map[string]string{"ID": "ID"}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, name FROM users WHERE id = ?", query.SQL)
	assertly.AssertValues(t, []interface{}{7}, query.Args)
}

func TestBuilder_BuildRejectsAmbiguousTemplateAndRelationBindings(t *testing.T) {
	type input struct{ ID int }
	evaluator, err := (sqltemplate.Compiler{
		Source:    `#set($value = $ID) SELECT $value`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	_, err = NewBuilder().Build(context.Background(),
		WithBuilderSQL(`SELECT 1`),
		WithBuilderInput(reflect.ValueOf(input{ID: 7})),
		WithBuilderTemplate(evaluator),
		WithBuilderPositionalArgs([]any{9}),
	)
	if err == nil || !strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("expected ambiguous binding error, got %v", err)
	}
}

func TestBuilder_BuildExecutesCompiledNamedParentNonWindowSQL(t *testing.T) {
	type input struct {
		Enabled bool
		Prefix  int
		Suffix  int
	}
	evaluator, err := (sqltemplate.Compiler{
		Source: `#if($Enabled) SELECT $Prefix AS prefix_value, COUNT(*) AS total
FROM ($View.Users.NonWindowSQL) parent
WHERE parent.id <= $Suffix #end`,
		InputType:        reflect.TypeOf(input{}),
		NonWindowAliases: []string{"Users"},
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderSQL("ignored after template evaluation"),
		WithBuilderInput(reflect.ValueOf(input{Enabled: true, Prefix: 3, Suffix: 9})),
		withTestParameterProjection(t, reflect.TypeOf(input{}), map[string]string{"Prefix": "Prefix", "Suffix": "Suffix"}),
		WithBuilderTemplate(evaluator),
		WithBuilderParentQuery(&cache.ParmetrizedQuery{
			SQL:  "SELECT id FROM users WHERE tenant_id = ? AND active = ?",
			Args: []interface{}{7, true},
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	wantSQL := "SELECT ? AS prefix_value, COUNT(*) AS total\nFROM (SELECT id FROM users WHERE tenant_id = ? AND active = ?) parent\nWHERE parent.id <= ?"
	if strings.TrimSpace(query.SQL) != wantSQL {
		t.Fatalf("unexpected SQL: %q", query.SQL)
	}
	if !reflect.DeepEqual(query.Args, []interface{}{3, 7, true, 9}) {
		t.Fatalf("unexpected args: %#v", query.Args)
	}
}

func TestBuilder_BuildExecutesParentViewWindow(t *testing.T) {
	type input struct{}
	evaluator, err := (sqltemplate.Compiler{
		Source: `SELECT $View.Limit AS limit_value, $View.Offset AS offset_value, $View.Page AS page_value
FROM ($View.Users.NonWindowSQL) parent`,
		InputType:        reflect.TypeOf(input{}),
		NonWindowAliases: []string{"Users"},
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if evaluator == nil {
		t.Fatal("expected $View template evaluator")
	}
	evaluated, err := evaluator.Evaluate(context.Background(), sqltemplate.Invocation{
		Input: reflect.ValueOf(input{}),
		View: sqltemplate.ViewInput{
			Limit: 7, Offset: 14, Page: 3,
			NonWindowSQL: "SELECT id FROM users",
		},
	})
	if err != nil {
		t.Fatalf("evaluate failed: %v", err)
	}
	wantEvaluatedSQL := "SELECT 7 AS limit_value, 14 AS offset_value, 3 AS page_value\nFROM (SELECT id FROM users) parent"
	if strings.TrimSpace(evaluated.SQL) != wantEvaluatedSQL {
		t.Fatalf("unexpected evaluated SQL: %q", evaluated.SQL)
	}
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderTemplate(evaluator),
		WithBuilderSelector(&xstate.Selector{Limit: 99, Offset: 98, Page: 97}),
		WithBuilderParentQuery(&cache.ParmetrizedQuery{
			SQL:   "SELECT id FROM users",
			Limit: 7, Offset: 14,
		}),
		WithBuilderParentSelector(&xstate.Selector{Page: 3}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	wantSQL := wantEvaluatedSQL + " LIMIT 99 OFFSET 98"
	if strings.TrimSpace(query.SQL) != wantSQL {
		t.Fatalf("unexpected SQL: %q", query.SQL)
	}
}

func TestBuilder_Build_BindsPositionalArgsAndMatcher(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, user_id, name FROM accounts WHERE user_id IN (?)"),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderPositionalArgs([]any{7, 8}),
		WithBuilderMatcher("user_id", []any{7, 8}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, user_id, name FROM accounts WHERE user_id IN (?,?)", query.SQL)
	assertly.AssertValues(t, []interface{}{7, 8}, query.Args)
	assertly.AssertValues(t, "user_id", query.By)
	assertly.AssertValues(t, []interface{}{7, 8}, query.In)
}

func TestBuilder_BuildExecutesCompiledViewParentJoinOn(t *testing.T) {
	type input struct{ Enabled bool }
	evaluator, err := (sqltemplate.Compiler{
		Source:    `#if($Enabled) SELECT id, vendor_id FROM product WHERE 1=1 $View.ParentJoinOn("AND", "vendor_id") #end`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	relation := &data.Relation{Of: &data.RelationRef{On: data.Links{{Field: "VendorID", Column: "vendor_id"}}}}
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderSQL("unused compiled source"),
		WithBuilderInput(reflect.ValueOf(input{Enabled: true})),
		WithBuilderTemplate(evaluator),
		WithBuilderRelation(relation),
		WithBuilderPositionalArgs([]any{7, 9}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if !strings.Contains(query.SQL, "AND vendor_id IN (?, ?)") {
		t.Fatalf("unexpected SQL: %s", query.SQL)
	}
	assertly.AssertValues(t, []interface{}{7, 9}, query.Args)
	assertly.AssertValues(t, "vendor_id", query.By)
	assertly.AssertValues(t, []interface{}{7, 9}, query.In)
}

func TestBuilder_CacheSQLExcludesCompiledViewParentJoinOn(t *testing.T) {
	type input struct{ Enabled bool }
	evaluator, err := (sqltemplate.Compiler{
		Source:    `#if($Enabled) SELECT id, vendor_id FROM product WHERE 1=1 $View.ParentJoinOn("AND", "vendor_id") #end`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	relation := &data.Relation{Of: &data.RelationRef{On: data.Links{{Field: "VendorID", Column: "vendor_id"}}}}
	query, err := NewBuilder().CacheSQL(context.Background(),
		WithBuilderSQL("unused compiled source"),
		WithBuilderInput(reflect.ValueOf(input{Enabled: true})),
		WithBuilderTemplate(evaluator),
		WithBuilderRelation(relation),
		WithBuilderPositionalArgs([]any{7, 9}),
	)
	if err != nil {
		t.Fatalf("cache build failed: %v", err)
	}
	if strings.Contains(query.SQL, "$View.") || strings.Contains(query.SQL, "vendor_id IN") || len(query.Args) != 0 {
		t.Fatalf("unexpected cache SQL: %s args=%#v", query.SQL, query.Args)
	}
	assertly.AssertValues(t, "vendor_id", query.By)
	assertly.AssertValues(t, []interface{}{7, 9}, query.In)
}

func TestBuilder_BuildExecutesCompiledViewParentCompositeJoinOn_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE signal (id INTEGER, kind TEXT, value TEXT);`,
		`INSERT INTO signal(id, kind, value) VALUES (1, 'country', 'PL'), (2, 'country', 'US'), (3, 'region', 'EU');`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	type input struct{ Enabled bool }
	evaluator, err := (sqltemplate.Compiler{
		Source:    `#if($Enabled) SELECT id, kind, value FROM signal WHERE 1=1 $View.ParentCompositeJoinOn("AND", "kind", "value") #end`,
		InputType: reflect.TypeOf(input{}),
	}).Compile()
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	relation := &data.Relation{Of: &data.RelationRef{On: data.Links{
		{Field: "Kind", Column: "kind"},
		{Field: "Value", Column: "value"},
	}}}
	rows := [][]interface{}{{"country", "PL"}, {"country", "US"}}
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderSQL("unused compiled source"),
		WithBuilderInput(reflect.ValueOf(input{Enabled: true})),
		WithBuilderTemplate(evaluator),
		WithBuilderRelation(relation),
		WithBuilderCompositeArgs([]string{"kind", "value"}, rows),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if strings.Count(query.SQL, "(kind, value) IN") != 1 {
		t.Fatalf("expected one composite parent filter, got %s", query.SQL)
	}
	assertly.AssertValues(t, []interface{}{"country", "PL", "country", "US"}, query.Args)
	if actual := countRows(t, h.DB, query.SQL, query.Args...); actual != 2 {
		t.Fatalf("expected two composite matches, got %d", actual)
	}
}

func TestBuilder_Build_ExpandsOriginalParentJoinOn_SQLite(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE product (id INTEGER, name TEXT, vendor_id INTEGER);`,
		`INSERT INTO product(id, name, vendor_id) VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30);`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	relation := &data.Relation{Of: &data.RelationRef{On: data.Links{{Field: "VendorID", Column: "vendor_id"}}}}
	sqlText := `SELECT id, name, vendor_id FROM product WHERE 1=1 $View.ParentJoinOn("AND","vendor_id")
UNION ALL
SELECT id, name, vendor_id FROM product WHERE 1=1 $View.ParentJoinOn("AND","vendor_id")`
	query, err := NewBuilder().Build(context.Background(),
		WithBuilderSQL(sqlText),
		WithBuilderInput(reflect.ValueOf(struct{}{})),
		WithBuilderRelation(relation),
		WithBuilderPositionalArgs([]any{10, 30}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if strings.Count(query.SQL, "vendor_id IN (?, ?)") != 2 {
		t.Fatalf("expected both original helpers to expand: %s", query.SQL)
	}
	assertly.AssertValues(t, []interface{}{10, 30, 10, 30}, query.Args)
	assertly.AssertValues(t, "vendor_id", query.By)
	rows := countRows(t, h.DB, query.SQL, query.Args...)
	if rows != 4 {
		t.Fatalf("expected two matching rows from each union branch, got %d", rows)
	}
}

func TestBuilder_CacheSQL_StripsOriginalParentJoinOnAndKeepsMatcher(t *testing.T) {
	relation := &data.Relation{Of: &data.RelationRef{On: data.Links{{Field: "VendorID", Column: "vendor_id"}}}}
	query, err := NewBuilder().CacheSQL(context.Background(),
		WithBuilderSQL(`SELECT id FROM product WHERE 1=1 $View.ParentJoinOn("AND","vendor_id")`),
		WithBuilderInput(reflect.ValueOf(struct{}{})),
		WithBuilderRelation(relation),
		WithBuilderPositionalArgs([]any{10, 30}),
	)
	if err != nil {
		t.Fatalf("cache build failed: %v", err)
	}
	if strings.Contains(query.SQL, "$View.") || strings.Contains(query.SQL, "vendor_id IN") {
		t.Fatalf("expected cache SQL to strip parent helper: %s", query.SQL)
	}
	assertly.AssertValues(t, "vendor_id", query.By)
	assertly.AssertValues(t, []interface{}{10, 30}, query.In)
}

func TestBuilder_Build_AppliesViewControls(t *testing.T) {
	type input struct{}
	limit := 10
	offset := 5

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name FROM users"),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderControls(&spec.ViewControls{
			OrderBy: "name DESC",
			Limit:   &limit,
			Offset:  &offset,
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, name FROM users ORDER BY name DESC LIMIT 10 OFFSET 5", query.SQL)
	assertly.AssertValues(t, []interface{}{}, query.Args)
}

func TestBuilder_Build_PrefersSelectorOverViewControls(t *testing.T) {
	type input struct{}
	limit := 10
	offset := 5

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name FROM users"),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderControls(&spec.ViewControls{
			OrderBy: "name DESC",
			Limit:   &limit,
			Offset:  &offset,
		}),
		WithBuilderSelector(&xstate.Selector{
			OrderBy: "id ASC",
			Limit:   3,
			Offset:  1,
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, name FROM users ORDER BY id ASC LIMIT 3 OFFSET 1", query.SQL)
}

func TestBuilder_Build_AppendsCompositeWhere(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, account_id, text FROM account_notes"),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderCompositeArgs([]string{"account_id", "id"}, [][]interface{}{
			{1, 10},
			{2, 20},
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, account_id, text FROM account_notes WHERE (account_id, id) IN ((?, ?), (?, ?))", query.SQL)
	assertly.AssertValues(t, []interface{}{1, 10, 2, 20}, query.Args)
}

func TestBuilder_Build_EmptyCompositeRowsBecomeNoMatch(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, account_id, text FROM account_notes"),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderCompositeArgs([]string{"account_id", "id"}, nil),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, account_id, text FROM account_notes WHERE 1 = 0", query.SQL)
	assertly.AssertValues(t, []interface{}(nil), query.Args)
}

func TestBuilder_Build_EmptyCompositeRowsInsertBeforeOrderBy(t *testing.T) {
	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, account_id FROM account_notes ORDER BY id"),
		WithBuilderInput(reflect.ValueOf(struct{}{})),
		WithBuilderCompositeArgs([]string{"account_id", "id"}, nil),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	assertly.AssertValues(t, "SELECT id, account_id FROM account_notes WHERE 1 = 0 ORDER BY id", query.SQL)
	assertly.AssertValues(t, []interface{}(nil), query.Args)
}

func TestBuilder_Build_CompositeFallbackPreservesArgumentOrderBeforeHaving(t *testing.T) {
	type input struct{ Min int }
	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT account_id, COUNT(*) AS total FROM account_notes GROUP BY account_id HAVING COUNT(*) > :Min ORDER BY account_id"),
		WithBuilderInput(reflect.ValueOf(input{Min: 2})),
		withTestParameterProjection(t, reflect.TypeOf(input{}), map[string]string{"Min": "Min"}),
		WithBuilderCompositeArgs([]string{"tenant_id", "account_id"}, [][]interface{}{{1, 7}}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}
	assertly.AssertValues(t, "SELECT account_id, COUNT(*) AS total FROM account_notes WHERE (tenant_id, account_id) IN ((?, ?)) GROUP BY account_id HAVING COUNT(*) > ? ORDER BY account_id", query.SQL)
	assertly.AssertValues(t, []interface{}{1, 7, 2}, query.Args)
}

func TestBuilder_Build_DerivesMatcherByFromRelation(t *testing.T) {
	type input struct{}

	relation := &data.Relation{
		Of: &data.RelationRef{
			On: data.Links{
				{Field: "UserID", Column: "user_id"},
			},
		},
	}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, user_id, name FROM accounts WHERE user_id IN (?)"),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderRelation(relation),
		WithBuilderPositionalArgs([]any{7, 8}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "user_id", query.By)
	assertly.AssertValues(t, []interface{}{7, 8}, query.In)
}

func TestBuilder_Build_BindsWhereSelectorCriteria(t *testing.T) {
	type input struct {
		Tenant int
	}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name FROM users WHERE tenant_id = :Tenant $AND_SELECTOR_CRITERIA ORDER BY id"),
		WithBuilderView(&data.View{Columns: []*data.Column{{Name: "name"}, {Name: "active"}}}),
		WithBuilderInput(reflect.ValueOf(input{Tenant: 9})),
		withTestParameterProjection(t, reflect.TypeOf(input{}), map[string]string{"Tenant": "Tenant"}),
		WithBuilderSelector(&xstate.Selector{
			Criteria:     "name = ? AND active = ?",
			Placeholders: []interface{}{"john", true},
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, name FROM users WHERE tenant_id = ?  AND (name = ? AND active = ?) ORDER BY id", query.SQL)
	assertly.AssertValues(t, []interface{}{9, "john", true}, query.Args)
}

func TestBuilder_Build_BindsSelectorCriteriaWithPositionalArgs(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, user_id, name FROM accounts WHERE user_id IN (?) $AND_SELECTOR_CRITERIA ORDER BY id"),
		WithBuilderView(&data.View{Columns: []*data.Column{{Name: "active"}}}),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderPositionalArgs([]any{7, 8}),
		WithBuilderSelector(&xstate.Selector{
			Criteria:     "active = ?",
			Placeholders: []interface{}{true},
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, user_id, name FROM accounts WHERE user_id IN (?,?)  AND (active = ?) ORDER BY id", query.SQL)
	assertly.AssertValues(t, []interface{}{7, 8, true}, query.Args)
}

func TestBuilder_Build_AppendsImplicitWhereSelectorCriteria(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name FROM users ORDER BY id"),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderSelector(&xstate.Selector{
			Criteria:     "name LIKE ?",
			Placeholders: []interface{}{"a%"},
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, name FROM users WHERE (name LIKE ?) ORDER BY id", query.SQL)
	assertly.AssertValues(t, []interface{}{"a%"}, query.Args)
}

func TestBuilder_Build_AppendsImplicitAndSelectorCriteria(t *testing.T) {
	type input struct {
		TenantID int
	}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name FROM users WHERE tenant_id = :TenantID ORDER BY id"),
		WithBuilderView(&data.View{Columns: []*data.Column{{Name: "active"}}}),
		WithBuilderInput(reflect.ValueOf(input{TenantID: 7})),
		withTestParameterProjection(t, reflect.TypeOf(input{}), map[string]string{"TenantID": "TenantID"}),
		WithBuilderSelector(&xstate.Selector{
			Criteria:     "active = ?",
			Placeholders: []interface{}{true},
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, name FROM users WHERE tenant_id = ? AND (active = ?) ORDER BY id", query.SQL)
	assertly.AssertValues(t, []interface{}{7, true}, query.Args)
}

func TestBuilder_Build_AppendsImplicitSelectorCriteriaBeforeGroupBy(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT tenant_id, COUNT(*) AS total FROM users GROUP BY tenant_id ORDER BY tenant_id"),
		WithBuilderView(&data.View{Columns: []*data.Column{{Name: "name"}}}),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderSelector(&xstate.Selector{
			Criteria:     "name LIKE ?",
			Placeholders: []interface{}{"a%"},
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT tenant_id, COUNT(*) AS total FROM users WHERE (name LIKE ?) GROUP BY tenant_id ORDER BY tenant_id", query.SQL)
	assertly.AssertValues(t, []interface{}{"a%"}, query.Args)
}

func TestBuilder_Build_AppliesSelectorProjection(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name, tenant_id FROM users ORDER BY id"),
		WithBuilderProjection([]string{"name"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT name FROM users ORDER BY id", query.SQL)
}

func TestBuilder_Build_AppliesSelectorFieldProjection(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name, tenant_id FROM users ORDER BY id"),
		WithBuilderSelector(&xstate.Selector{Fields: []string{"name"}}),
		WithBuilderProjection([]string{"name"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT name FROM users ORDER BY id", query.SQL)
}

func TestBuilder_Build_AppliesSelectorProjectionBySQLAlias(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name AS display_name, tenant_id FROM users ORDER BY id"),
		WithBuilderProjection([]string{"display_name"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT name AS display_name FROM users ORDER BY id", query.SQL)
}

func TestBuilder_Build_AppliesSelectorProjectionByExpressionAlias(t *testing.T) {
	type input struct{}

	useCases := []struct {
		description string
		sqlText     string
		selected    []string
		expectSQL   string
	}{
		{
			description: "case expression keeps alias projection",
			sqlText:     "SELECT id, (CASE WHEN name <> '' THEN name ELSE NULL END) AS display_name, tenant_id FROM users ORDER BY id",
			selected:    []string{"display_name"},
			expectSQL:   "SELECT (CASE WHEN name <> '' THEN name ELSE NULL END) AS display_name FROM users ORDER BY id",
		},
		{
			description: "coalesce expression keeps alias projection",
			sqlText:     "SELECT id, COALESCE(name, 'n/a') AS display_name, tenant_id FROM users ORDER BY id",
			selected:    []string{"display_name"},
			expectSQL:   "SELECT COALESCE(name, 'n/a') AS display_name FROM users ORDER BY id",
		},
		{
			description: "distinct projection preserves selection kind",
			sqlText:     "SELECT DISTINCT id, COALESCE(name, 'n/a') AS display_name FROM users ORDER BY id",
			selected:    []string{"display_name"},
			expectSQL:   "SELECT DISTINCT COALESCE(name, 'n/a') AS display_name FROM users ORDER BY id",
		},
	}

	for _, useCase := range useCases {
		t.Run(useCase.description, func(t *testing.T) {
			query, err := NewBuilder().Build(
				context.Background(),
				WithBuilderSQL(useCase.sqlText),
				WithBuilderSelector(&xstate.Selector{Columns: useCase.selected}),
				WithBuilderProjection(useCase.selected),
				WithBuilderInput(reflect.ValueOf(input{})),
			)
			if err != nil {
				t.Fatalf("build failed: %v", err)
			}
			assertly.AssertValues(t, useCase.expectSQL, query.SQL)
		})
	}
}

func TestBuilder_Build_GroupedProjectionRewritesGroupByAndOrderBy(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT tenant_id, name, COUNT(*) AS total FROM users GROUP BY tenant_id, name ORDER BY tenant_id, name"),
		WithBuilderView(resolvedGroupableView()),
		WithBuilderSelector(&xstate.Selector{Columns: []string{"tenant_id", "total"}}),
		WithBuilderProjection([]string{"tenant_id", "total"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t,
		normalizeSQLForAssert("SELECT tenant_id, COUNT(*) AS total FROM users GROUP BY 1 ORDER BY tenant_id"),
		normalizeSQLForAssert(query.SQL),
	)
}

func resolvedGroupableView() *data.View {
	groupable := true
	return data.FromComponent(&spec.Component{RootView: &spec.View{Groupable: &groupable}})
}

func TestBuilder_Build_OrdinaryProjectionPreservesAuthoredGrouping(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT tenant_id, name, COUNT(*) AS total FROM users GROUP BY tenant_id, name ORDER BY tenant_id, name"),
		WithBuilderView(&data.View{}),
		WithBuilderProjection([]string{"tenant_id", "total"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t,
		normalizeSQLForAssert("SELECT tenant_id, COUNT(*) AS total FROM users GROUP BY tenant_id, name ORDER BY tenant_id, name"),
		normalizeSQLForAssert(query.SQL),
	)
}

func TestBuilder_ShapeBound_HonorsExplicitGroupableMetadata(t *testing.T) {
	source := &cache.ParmetrizedQuery{SQL: "SELECT tenant_id, name, COUNT(*) AS total FROM users GROUP BY tenant_id, name ORDER BY tenant_id, name"}
	tests := []struct {
		name string
		view *data.View
		want string
	}{
		{
			name: "ordinary view preserves authored grouping",
			view: data.FromComponent(&spec.Component{RootView: &spec.View{}}),
			want: "SELECT tenant_id, COUNT(*) AS total FROM users GROUP BY tenant_id, name ORDER BY tenant_id, name",
		},
		{
			name: "groupable view rewrites grouping",
			view: resolvedGroupableView(),
			want: "SELECT tenant_id, COUNT(*) AS total FROM users GROUP BY 1 ORDER BY tenant_id",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := NewBuilder().ShapeBound(
				source,
				WithBuilderView(test.view),
				WithBuilderProjection([]string{"tenant_id", "total"}),
			)
			if err != nil {
				t.Fatalf("ShapeBound() error = %v", err)
			}
			assertly.AssertValues(t, normalizeSQLForAssert(test.want), normalizeSQLForAssert(actual.SQL))
		})
	}
}

func TestBuilder_Build_GroupedProjectionDropsGroupByForAggregateOnlySelection(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT tenant_id, COUNT(*) AS total FROM users GROUP BY tenant_id ORDER BY tenant_id"),
		WithBuilderView(resolvedGroupableView()),
		WithBuilderSelector(&xstate.Selector{Columns: []string{"total"}}),
		WithBuilderProjection([]string{"total"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t,
		normalizeSQLForAssert("SELECT COUNT(*) AS total FROM users"),
		normalizeSQLForAssert(query.SQL),
	)
}

func TestBuilder_Build_GroupedProjectionPrunesCTEDimensionsAndOrderBy(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("WITH last_n AS (SELECT tenant_id, name, amount FROM users) SELECT tenant_id, name, SUM(amount) AS total FROM last_n GROUP BY tenant_id, name ORDER BY name, tenant_id LIMIT 1000"),
		WithBuilderView(resolvedGroupableView()),
		WithBuilderSelector(&xstate.Selector{Columns: []string{"tenant_id", "total"}}),
		WithBuilderProjection([]string{"tenant_id", "total"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t,
		normalizeSQLForAssert("WITH last_n AS (SELECT tenant_id, name, amount FROM users) SELECT tenant_id, SUM(amount) AS total FROM last_n GROUP BY 1 ORDER BY tenant_id LIMIT 1000"),
		normalizeSQLForAssert(query.SQL),
	)
}

func TestBuilder_Build_GroupedProjectionRecognizesNestedAggregateExpression(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT tenant_id, ROUND(SUM(amount), 2) AS total FROM users GROUP BY tenant_id ORDER BY tenant_id"),
		WithBuilderView(resolvedGroupableView()),
		WithBuilderSelector(&xstate.Selector{Columns: []string{"total"}}),
		WithBuilderProjection([]string{"total"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t,
		normalizeSQLForAssert("SELECT ROUND(SUM(amount), 2) AS total FROM users"),
		normalizeSQLForAssert(query.SQL),
	)
}

func TestBuilder_Build_GroupedCTEProjectionKeepsSelectedNonAggregateDimension(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("WITH last_n AS (SELECT tenant_id, name, category, amount FROM users) SELECT tenant_id, category, SUM(amount) AS total FROM last_n GROUP BY tenant_id, name, category ORDER BY tenant_id LIMIT 1000"),
		WithBuilderView(resolvedGroupableView()),
		WithBuilderSelector(&xstate.Selector{Columns: []string{"tenant_id", "category", "total"}}),
		WithBuilderProjection([]string{"tenant_id", "category", "total"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t,
		normalizeSQLForAssert("WITH last_n AS (SELECT tenant_id, name, category, amount FROM users) SELECT tenant_id, category, SUM(amount) AS total FROM last_n GROUP BY 1, 2 ORDER BY tenant_id LIMIT 1000"),
		normalizeSQLForAssert(query.SQL),
	)
}

func TestBuilder_Build_GroupedReportProjectionDropsOrderByOnPrunedDimension(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("WITH last_n AS (SELECT tenant_id, name, amount FROM users) SELECT name, SUM(amount) AS total FROM last_n GROUP BY name, tenant_id ORDER BY tenant_id LIMIT 1000"),
		WithBuilderView(resolvedGroupableView()),
		WithBuilderSelector(&xstate.Selector{Columns: []string{"total"}}),
		WithBuilderProjection([]string{"total"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t,
		normalizeSQLForAssert("WITH last_n AS (SELECT tenant_id, name, amount FROM users) SELECT SUM(amount) AS total FROM last_n LIMIT 1000"),
		normalizeSQLForAssert(query.SQL),
	)
}

func TestBuilder_Build_GroupedProjectionKeepsOrderBySelectedAggregateAlias(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT tenant_id, SUM(amount) AS total FROM users GROUP BY tenant_id ORDER BY total DESC"),
		WithBuilderView(resolvedGroupableView()),
		WithBuilderSelector(&xstate.Selector{Columns: []string{"total"}}),
		WithBuilderProjection([]string{"total"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t,
		normalizeSQLForAssert("SELECT SUM(amount) AS total FROM users ORDER BY total DESC"),
		normalizeSQLForAssert(query.SQL),
	)
}

func TestBuilder_Build_GroupedProjectionWithSelectorCriteria(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT tenant_id, name, SUM(amount) AS total FROM users GROUP BY tenant_id, name ORDER BY total DESC"),
		WithBuilderView(resolvedGroupableView()),
		WithBuilderSelector(&xstate.Selector{
			Columns:      []string{"tenant_id", "total"},
			Criteria:     "name LIKE ?",
			Placeholders: []interface{}{"a%"},
		}),
		WithBuilderProjection([]string{"tenant_id", "total"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t,
		normalizeSQLForAssert("SELECT tenant_id, SUM(amount) AS total FROM users WHERE (name LIKE ?) GROUP BY 1 ORDER BY total DESC"),
		normalizeSQLForAssert(query.SQL),
	)
	assertly.AssertValues(t, []interface{}{"a%"}, query.Args)
}

func TestBuilder_Build_GroupedProjectionAddsGroupByWhenMissing(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT tenant_id, SUM(amount) AS total FROM users ORDER BY total DESC"),
		WithBuilderView(resolvedGroupableView()),
		WithBuilderSelector(&xstate.Selector{Columns: []string{"tenant_id", "total"}}),
		WithBuilderProjection([]string{"tenant_id", "total"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t,
		normalizeSQLForAssert("SELECT tenant_id, SUM(amount) AS total FROM users GROUP BY 1 ORDER BY total DESC"),
		normalizeSQLForAssert(query.SQL),
	)
}

func TestBuilder_Build_AppliesSelectorFieldProjectionBySQLAlias(t *testing.T) {
	type input struct{}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name AS display_name, tenant_id FROM users ORDER BY id"),
		WithBuilderSelector(&xstate.Selector{Fields: []string{"display_name"}}),
		WithBuilderProjection([]string{"display_name"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT name AS display_name FROM users ORDER BY id", query.SQL)
}

func TestBuilder_Build_ErrorsOnMissingSelectedColumn(t *testing.T) {
	type input struct{}

	_, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name FROM users ORDER BY id"),
		WithBuilderProjection([]string{"missing"}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err == nil || !strings.Contains(err.Error(), "not found column") {
		t.Fatalf("expected missing selected column error, got %v", err)
	}
}

func TestBuilder_Build_ReplacesPaginationToken(t *testing.T) {
	type input struct{}
	limit := 2
	offset := 1

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, name FROM (SELECT * FROM users ORDER BY id $PAGINATION) t"),
		WithBuilderControls(&spec.ViewControls{
			Limit:  &limit,
			Offset: &offset,
		}),
		WithBuilderInput(reflect.ValueOf(input{})),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, name FROM (SELECT * FROM users ORDER BY id  LIMIT 2 OFFSET 1) t", query.SQL)
}

func TestBuilder_Build_ReplacesAndColumnInToken(t *testing.T) {
	type input struct {
		ID int
	}
	relation := &data.Relation{
		Of: &data.RelationRef{
			On: data.Links{
				{Column: "user_id"},
			},
		},
	}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, user_id, name FROM accounts WHERE id = :ID $AND_COLUMN_IN ORDER BY id"),
		WithBuilderInput(reflect.ValueOf(input{ID: 10})),
		withTestParameterProjection(t, reflect.TypeOf(input{}), map[string]string{"ID": "ID"}),
		WithBuilderRelation(relation),
		WithBuilderPositionalArgs([]any{7, 8}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, user_id, name FROM accounts WHERE id = ?  AND ( user_id IN (?,?)) ORDER BY id", query.SQL)
	assertly.AssertValues(t, []interface{}{10, 7, 8}, query.Args)
}

func TestBuilder_Build_ReplacesColumnInToken(t *testing.T) {
	type input struct{}
	relation := &data.Relation{
		Of: &data.RelationRef{
			On: data.Links{
				{Column: "user_id"},
			},
		},
	}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, user_id, name FROM accounts WHERE $COLUMN_IN ORDER BY id"),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderRelation(relation),
		WithBuilderPositionalArgs([]any{7, 8}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, user_id, name FROM accounts WHERE user_id IN (?,?) ORDER BY id", query.SQL)
	assertly.AssertValues(t, []interface{}{7, 8}, query.Args)
}

func TestBuilder_Build_ReplacesCompositeColumnInToken(t *testing.T) {
	type input struct{}
	relation := &data.Relation{
		Of: &data.RelationRef{
			On: data.Links{
				{Column: "feature_type"},
				{Column: "value"},
			},
		},
	}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT feature_type, value, metric FROM signal_performance WHERE $COLUMN_IN ORDER BY feature_type"),
		WithBuilderInput(reflect.ValueOf(input{})),
		WithBuilderRelation(relation),
		WithBuilderCompositeArgs([]string{"feature_type", "value"}, [][]interface{}{
			{"country", "PL"},
			{"country", "US"},
		}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT feature_type, value, metric FROM signal_performance WHERE (feature_type, value) IN ((?, ?), (?, ?)) ORDER BY feature_type", query.SQL)
	assertly.AssertValues(t, []interface{}{"country", "PL", "country", "US"}, query.Args)
}

func TestBuilder_Build_UsesDialectForExplicitCompositeRelationMarkers(t *testing.T) {
	relation := &data.Relation{Of: &data.RelationRef{On: data.Links{{Column: "feature_type"}, {Column: "value"}}}}
	dialect := &info.Dialect{CompositeInRenderer: func(columns []string, rowCount int) string {
		return "dialect_composite_match(?, ?, ?, ?)"
	}}
	rows := [][]interface{}{{"country", "PL"}, {"country", "US"}}
	for _, testCase := range []struct {
		name    string
		sqlText string
		wantSQL string
	}{
		{
			name:    "column in marker",
			sqlText: "SELECT feature_type, value FROM signal_performance WHERE $COLUMN_IN",
			wantSQL: "SELECT feature_type, value FROM signal_performance WHERE dialect_composite_match(?, ?, ?, ?)",
		},
		{
			name:    "criteria marker",
			sqlText: "SELECT feature_type, value FROM signal_performance $WHERE_CRITERIA",
			wantSQL: "SELECT feature_type, value FROM signal_performance WHERE dialect_composite_match(?, ?, ?, ?)",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			query, err := NewBuilder().Build(
				context.Background(),
				WithBuilderSQL(testCase.sqlText),
				WithBuilderInput(reflect.ValueOf(struct{}{})),
				WithBuilderRelation(relation),
				WithBuilderCompositeArgs([]string{"feature_type", "value"}, rows),
				WithBuilderDialect(dialect),
			)
			if err != nil {
				t.Fatalf("build failed: %v", err)
			}
			assertly.AssertValues(t, testCase.wantSQL, query.SQL)
			assertly.AssertValues(t, []interface{}{"country", "PL", "country", "US"}, query.Args)
		})
	}
}

func TestBuilder_Build_AppendsRelationColumnInWhenMissing(t *testing.T) {
	type input struct {
		ID int
	}
	relation := &data.Relation{
		Of: &data.RelationRef{
			On: data.Links{
				{Column: "user_id"},
			},
		},
	}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, user_id, name FROM accounts WHERE id = :ID ORDER BY id"),
		WithBuilderInput(reflect.ValueOf(input{ID: 10})),
		withTestParameterProjection(t, reflect.TypeOf(input{}), map[string]string{"ID": "ID"}),
		WithBuilderRelation(relation),
		WithBuilderPositionalArgs([]any{7, 8}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, user_id, name FROM accounts WHERE id = ? AND user_id IN (?,?) ORDER BY id", query.SQL)
	assertly.AssertValues(t, []interface{}{10, 7, 8}, query.Args)
}

func TestBuilder_Build_ReplacesWhereCriteriaTokenFromRelation(t *testing.T) {
	type input struct {
		ID int
	}
	relation := &data.Relation{
		Of: &data.RelationRef{
			On: data.Links{
				{Column: "user_id"},
			},
		},
	}

	query, err := NewBuilder().Build(
		context.Background(),
		WithBuilderSQL("SELECT id, user_id, name FROM accounts $WHERE_CRITERIA ORDER BY id"),
		WithBuilderInput(reflect.ValueOf(input{ID: 10})),
		WithBuilderRelation(relation),
		WithBuilderPositionalArgs([]any{7, 8}),
	)
	if err != nil {
		t.Fatalf("build failed: %v", err)
	}

	assertly.AssertValues(t, "SELECT id, user_id, name FROM accounts WHERE user_id IN (?,?) ORDER BY id", query.SQL)
	assertly.AssertValues(t, []interface{}{7, 8}, query.Args)
}
