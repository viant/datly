package macro

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/sqlx/metadata/info"
)

func TestParentCompositeJoinOnUsesNativeDialect(t *testing.T) {
	for _, test := range []struct {
		name, source, want string
		dialect            *info.Dialect
		rows               [][]interface{}
	}{
		{"nil scalar", `$View.ParentCompositeJoinOn("","id")`, "id IN (?, ?)", nil, [][]interface{}{{1}, {2}}},
		{"nil tuples", `$View.ParentCompositeJoinOn("","tenant","id")`, "(tenant, id) IN ((?, ?), (?, ?))", nil, [][]interface{}{{1, 7}, {2, 7}}},
		{"custom tuples", `$View.ParentCompositeJoinOn("","tenant","id")`, "dialect_tuple_in", &info.Dialect{CompositeInRenderer: func([]string, int) string { return "dialect_tuple_in" }}, [][]interface{}{{1, 7}, {2, 7}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			actual, args, _, err := ExpandParentKeyCalls(test.source, test.dialect, nil, test.rows)
			if err != nil || actual != test.want || !reflect.DeepEqual(args, flattenRows(test.rows)) {
				t.Fatalf("SQL=%q args=%v err=%v", actual, args, err)
			}
		})
	}
}

func TestParentKeyCalls(t *testing.T) {
	sqlText := `SELECT * FROM product p WHERE 1=1
$View.ParentJoinOn("AND", "p.vendor_id")
$View.ParentCompositeJoinOn("OR", "p.kind", "p.value")
$View.AndParentJoinOn("p.account_id")`
	calls, err := ParentKeyCalls(sqlText)
	if err != nil {
		t.Fatalf("ParentKeyCalls failed: %v", err)
	}
	if len(calls) != 3 {
		t.Fatalf("expected three calls, got %d", len(calls))
	}
	if calls[0].Prefix != "AND" || !reflect.DeepEqual(calls[0].Columns, []string{"p.vendor_id"}) {
		t.Fatalf("unexpected scalar call: %+v", calls[0])
	}
	if calls[1].Prefix != "OR" || !reflect.DeepEqual(calls[1].Columns, []string{"p.kind", "p.value"}) {
		t.Fatalf("unexpected composite call: %+v", calls[1])
	}
	if calls[2].Prefix != "AND" || !reflect.DeepEqual(calls[2].Columns, []string{"p.account_id"}) {
		t.Fatalf("unexpected and call: %+v", calls[2])
	}
}

func TestExpandParentKeyCalls_RepeatedScalar(t *testing.T) {
	sqlText := `SELECT id FROM product WHERE 1=1 $View.ParentJoinOn("AND","vendor_id")
UNION ALL SELECT id FROM product WHERE 1=1 $View.ParentJoinOn("AND","vendor_id")`
	actual, args, count, err := ExpandParentKeyCalls(sqlText, nil, []any{7, 8}, nil)
	if err != nil {
		t.Fatalf("ExpandParentKeyCalls failed: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected two expansions, got %d", count)
	}
	if strings.Count(actual, "AND vendor_id IN (?, ?)") != 2 {
		t.Fatalf("unexpected expanded SQL: %s", actual)
	}
	if !reflect.DeepEqual(args, []any{7, 8, 7, 8}) {
		t.Fatalf("unexpected repeated args: %#v", args)
	}
}

func TestExpandParentKeyCalls_CompositeAndEmpty(t *testing.T) {
	sqlText := `SELECT id FROM signal WHERE 1=1 $View.ParentCompositeJoinOn("AND","kind","value")`
	actual, args, _, err := ExpandParentKeyCalls(sqlText, nil, nil, [][]interface{}{{"country", "PL"}, {"country", "US"}})
	if err != nil {
		t.Fatalf("ExpandParentKeyCalls failed: %v", err)
	}
	if !strings.Contains(actual, "AND (kind, value) IN ((?, ?), (?, ?))") {
		t.Fatalf("unexpected composite SQL: %s", actual)
	}
	if !reflect.DeepEqual(args, []any{"country", "PL", "country", "US"}) {
		t.Fatalf("unexpected composite args: %#v", args)
	}

	actual, args, _, err = ExpandParentKeyCalls(sqlText, nil, nil, nil)
	if err != nil {
		t.Fatalf("empty expansion failed: %v", err)
	}
	if !strings.Contains(actual, "AND 1 = 0") || len(args) != 0 {
		t.Fatalf("expected empty composite to become no-match, SQL=%s args=%v", actual, args)
	}
}

func TestStripParentKeyCalls(t *testing.T) {
	sqlText := `SELECT id FROM product WHERE 1=1 $View.ParentJoinOn("AND","vendor_id")`
	actual, count, err := StripParentKeyCalls(sqlText)
	if err != nil {
		t.Fatalf("StripParentKeyCalls failed: %v", err)
	}
	if count != 1 || strings.Contains(actual, "$View.") || !strings.Contains(actual, "WHERE 1=1") {
		t.Fatalf("unexpected stripped SQL: %s", actual)
	}
}

func TestParentKeyCalls_IgnoreProtectedSQLText(t *testing.T) {
	sqlText := `SELECT '$View.ParentJoinOn("AND","literal")' AS sample
-- $View.ParentJoinOn("AND","comment")
/* $View.ParentJoinOn("AND","block") */
FROM product WHERE 1=1 $View.ParentJoinOn("AND","vendor_id")`
	calls, err := ParentKeyCalls(sqlText)
	if err != nil {
		t.Fatalf("ParentKeyCalls failed: %v", err)
	}
	if len(calls) != 1 || calls[0].Columns[0] != "vendor_id" {
		t.Fatalf("unexpected calls: %+v", calls)
	}
	expanded, args, count, err := ExpandParentKeyCalls(sqlText, nil, []any{7}, nil)
	if err != nil {
		t.Fatalf("ExpandParentKeyCalls failed: %v", err)
	}
	if count != 1 || !strings.Contains(expanded, `'$View.ParentJoinOn("AND","literal")'`) || !strings.Contains(expanded, "AND vendor_id IN (?)") {
		t.Fatalf("unexpected expanded SQL: %s", expanded)
	}
	if !reflect.DeepEqual(args, []any{7}) {
		t.Fatalf("unexpected expanded args: %#v", args)
	}
	stripped, count, err := StripParentKeyCalls(sqlText)
	if err != nil {
		t.Fatalf("StripParentKeyCalls failed: %v", err)
	}
	if count != 1 || !strings.Contains(stripped, `'$View.ParentJoinOn("AND","literal")'`) || strings.Contains(stripped, "WHERE 1=1 $View.ParentJoinOn") {
		t.Fatalf("unexpected stripped SQL: %s", stripped)
	}
}

func TestExpandNonWindowSQL_IgnoreProtectedSQLText(t *testing.T) {
	sqlText := `SELECT '$View.Users.NonWindowSQL' AS sample
-- $View.Users.NonWindowSQL
FROM ($View.Users.NonWindowSQL) parent`
	actual, count := ExpandNonWindowSQL(sqlText, "SELECT id FROM users", "Users")
	if count != 1 || !strings.Contains(actual, `'$View.Users.NonWindowSQL'`) || !strings.Contains(actual, "FROM (SELECT id FROM users) parent") {
		t.Fatalf("unexpected non-window expansion: count=%d SQL=%s", count, actual)
	}
}

func TestPrepareNonWindowSQLTemplate(t *testing.T) {
	sqlText := `SELECT '$View.Users.NonWindowSQL' AS sample
-- $View.Users.NonWindowSQL
FROM ($View.Users.NonWindowSQL) named
JOIN ($View.NonWindowSQL) unqualified ON 1=1`
	actual, count := PrepareNonWindowSQLTemplate(sqlText, "Users")
	if count != 2 {
		t.Fatalf("expected two code-position rewrites, got %d: %s", count, actual)
	}
	if !strings.Contains(actual, `'#[[$View.Users.NonWindowSQL]]#'`) || !strings.Contains(actual, `-- #[[$View.Users.NonWindowSQL]]#`) {
		t.Fatalf("protected SQL text was not escaped for Velty: %s", actual)
	}
	if strings.Count(actual, "$View.NonWindowSQL()") != 2 {
		t.Fatalf("unexpected rewritten SQL: %s", actual)
	}
}

func TestPrepareNonWindowSQLTemplatePreservesMethodCallAndTokenBoundary(t *testing.T) {
	sqlText := `SELECT $View.Users.NonWindowSQL (), $View.NonWindowSQLExtra`
	actual, count := PrepareNonWindowSQLTemplate(sqlText, "Users")
	if count != 1 || !strings.Contains(actual, `$View.NonWindowSQL ()`) || !strings.Contains(actual, `$View.NonWindowSQLExtra`) {
		t.Fatalf("unexpected rewritten SQL: %s (count %d)", actual, count)
	}
}

func TestNonWindowSQLAccessesSkipProtectedTextAndReportAliases(t *testing.T) {
	sqlText := `SELECT '$View.Hidden.NonWindowSQL'
-- $View.Comment.NonWindowSQL
FROM ($View.Users.NonWindowSQL) named
JOIN ($View.NonWindowSQL) relative ON 1=1`
	accesses := NonWindowSQLAccesses(sqlText)
	if len(accesses) != 2 {
		t.Fatalf("expected two code accesses, got %#v", accesses)
	}
	if accesses[0].Alias != "Users" || accesses[0].Raw != "$View.Users.NonWindowSQL" || accesses[1].Alias != "" {
		t.Fatalf("unexpected accesses: %#v", accesses)
	}
	if err := ValidateNonWindowSQLAliases(sqlText, "Users"); err != nil {
		t.Fatalf("allowed alias failed validation: %v", err)
	}
}

func TestValidateNonWindowSQLAliasesRejectsUnknownCodeAlias(t *testing.T) {
	err := ValidateNonWindowSQLAliases(`SELECT * FROM ($View.Other.NonWindowSQL) parent`, "Users")
	if err == nil || !strings.Contains(err.Error(), `unknown parent view alias "Other"`) {
		t.Fatalf("expected unknown alias error, got %v", err)
	}
}

func TestParentKeyCalls_RejectsStructuralInjection(t *testing.T) {
	for _, sqlText := range []string{
		`SELECT 1 $View.ParentJoinOn("DROP", "id")`,
		`SELECT 1 $View.ParentJoinOn("AND", "id OR 1=1")`,
	} {
		if _, err := ParentKeyCalls(sqlText); err == nil {
			t.Fatalf("expected invalid helper to fail: %s", sqlText)
		}
	}
}

func TestExpandNonWindowSQL(t *testing.T) {
	testCases := []struct {
		name      string
		sql       string
		aliases   []string
		wantSQL   string
		wantCount int
	}{
		{
			name:      "no token",
			sql:       `SELECT 1`,
			wantSQL:   `SELECT 1`,
			wantCount: 0,
		},
		{
			name:      "generic token",
			sql:       `SELECT COUNT(1) FROM ($View.NonWindowSQL) t`,
			wantSQL:   `SELECT COUNT(1) FROM (SELECT id FROM users WHERE id >= :ID) t`,
			wantCount: 1,
		},
		{
			name:      "named token",
			sql:       `SELECT COUNT(1) FROM ($View.Users.NonWindowSQL) t`,
			aliases:   []string{"Users"},
			wantSQL:   `SELECT COUNT(1) FROM (SELECT id FROM users WHERE id >= :ID) t`,
			wantCount: 1,
		},
		{
			name:      "repeated token",
			sql:       `SELECT (SELECT COUNT(1) FROM ($View.NonWindowSQL) a) AS a, (SELECT COUNT(1) FROM ($View.Users.NonWindowSQL) b) AS b`,
			aliases:   []string{"Users"},
			wantSQL:   `SELECT (SELECT COUNT(1) FROM (SELECT id FROM users WHERE id >= :ID) a) AS a, (SELECT COUNT(1) FROM (SELECT id FROM users WHERE id >= :ID) b) AS b`,
			wantCount: 2,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			actual, count := ExpandNonWindowSQL(testCase.sql, `SELECT id FROM users WHERE id >= :ID`, testCase.aliases...)
			if actual != testCase.wantSQL {
				t.Fatalf("unexpected expanded sql: %q", actual)
			}
			if count != testCase.wantCount {
				t.Fatalf("unexpected replacement count: %d", count)
			}
		})
	}
}
