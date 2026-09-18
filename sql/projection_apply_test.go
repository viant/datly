package sql

import (
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlparser"
)

func TestApplySelectorProjection(t *testing.T) {
	useCases := []struct {
		name      string
		sqlText   string
		selected  []string
		groupable bool
		expectSQL string
		expectErr string
	}{
		{
			name:      "empty selection returns input",
			sqlText:   "SELECT id, name FROM users",
			selected:  nil,
			expectSQL: "SELECT id, name FROM users",
		},
		{
			name:      "parse failure rejects selection",
			sqlText:   "SELECT FROM",
			selected:  []string{"id"},
			expectErr: "source projection is unresolved",
		},
		{
			name:      "non select statement rejects selection",
			sqlText:   "UPDATE users SET name = 'x'",
			selected:  []string{"id"},
			expectErr: "source projection is unresolved",
		},
		{
			name:      "unknown selection without from rejects",
			sqlText:   "SELECT 1",
			selected:  []string{"id"},
			expectErr: "not found column id",
		},
		{
			name:      "filters projection by alias and duplicate selection",
			sqlText:   "SELECT id, name AS display_name, email FROM users",
			selected:  []string{" display_name ", "display_name", "display_name"},
			expectSQL: "SELECT name AS display_name FROM users",
		},
		{
			name:      "preserves distinct when filtering projection",
			sqlText:   "SELECT DISTINCT id, name FROM users",
			selected:  []string{"name"},
			expectSQL: "SELECT DISTINCT name FROM users",
		},
		{
			name:      "comment comma does not misalign projection source",
			sqlText:   "SELECT id /* source, identifier */, name FROM users",
			selected:  []string{"name"},
			expectSQL: "SELECT name FROM users",
		},
		{
			name:      "selecting all projected items returns input",
			sqlText:   "SELECT id, name FROM users",
			selected:  []string{"id", "name"},
			expectSQL: "SELECT id, name FROM users",
		},
		{
			name:      "grouped projection rewrites group by",
			sqlText:   "SELECT tenant_id, region, SUM(amount) AS total FROM sales GROUP BY tenant_id, region ORDER BY region, total",
			selected:  []string{"tenant_id", "total"},
			groupable: true,
			expectSQL: "SELECT tenant_id, SUM(amount) AS total FROM sales GROUP BY 1 ORDER BY total",
		},
		{
			name:      "ordinary projection preserves authored grouping",
			sqlText:   "SELECT tenant_id, region, SUM(amount) AS total FROM sales GROUP BY tenant_id, region ORDER BY region, total",
			selected:  []string{"tenant_id", "total"},
			expectSQL: "SELECT tenant_id, total FROM (SELECT tenant_id, region, SUM(amount) AS total FROM sales GROUP BY tenant_id, region ORDER BY region, total) AS datly_view",
		},
		{
			name:      "ordinary grouped projection wraps removed group and order dependencies",
			sqlText:   "SELECT ad_order_id, audience_id, COUNT(DISTINCT audience_id) AS audience_count, SUM(imps) AS total_imps_7d FROM audiences GROUP BY 1, 2 ORDER BY total_imps_7d DESC",
			selected:  []string{"ad_order_id", "audience_count"},
			expectSQL: "SELECT ad_order_id, audience_count FROM (SELECT ad_order_id, audience_id, COUNT(DISTINCT audience_id) AS audience_count, SUM(imps) AS total_imps_7d FROM audiences GROUP BY 1, 2 ORDER BY total_imps_7d DESC) AS datly_view",
		},
		{
			name:      "set query projects from complete union",
			sqlText:   "SELECT id, name FROM current_users UNION ALL SELECT user_id AS id, full_name AS name FROM archived_users",
			selected:  []string{"name"},
			expectSQL: "SELECT name FROM (SELECT id, name FROM current_users UNION ALL SELECT user_id AS id, full_name AS name FROM archived_users) AS datly_set",
		},
		{
			name:      "set query resolves expression alias",
			sqlText:   "SELECT id, COALESCE(name, 'unknown') AS display_name FROM current_users UNION ALL SELECT user_id AS id, full_name AS display_name FROM archived_users",
			selected:  []string{"display_name"},
			expectSQL: "SELECT display_name FROM (SELECT id, COALESCE(name, 'unknown') AS display_name FROM current_users UNION ALL SELECT user_id AS id, full_name AS display_name FROM archived_users) AS datly_set",
		},
		{
			name:      "set query preserves double quoted alias",
			sqlText:   `SELECT id, name AS "display_name" FROM current_users UNION ALL SELECT user_id AS id, full_name AS "display_name" FROM archived_users`,
			selected:  []string{"display_name"},
			expectSQL: `SELECT "display_name" FROM (SELECT id, name AS "display_name" FROM current_users UNION ALL SELECT user_id AS id, full_name AS "display_name" FROM archived_users) AS datly_set`,
		},
		{
			name:      "set query preserves backtick quoted alias",
			sqlText:   "SELECT id, name AS `display_name` FROM current_users UNION ALL SELECT user_id AS id, full_name AS `display_name` FROM archived_users",
			selected:  []string{"display_name"},
			expectSQL: "SELECT `display_name` FROM (SELECT id, name AS `display_name` FROM current_users UNION ALL SELECT user_id AS id, full_name AS `display_name` FROM archived_users) AS datly_set",
		},
		{
			name:      "set query preserves bracket quoted alias",
			sqlText:   "SELECT id, name AS [display_name] FROM current_users UNION ALL SELECT user_id AS id, full_name AS [display_name] FROM archived_users",
			selected:  []string{"display_name"},
			expectSQL: "SELECT [display_name] FROM (SELECT id, name AS [display_name] FROM current_users UNION ALL SELECT user_id AS id, full_name AS [display_name] FROM archived_users) AS datly_set",
		},
		{
			name:      "set query requires computed output alias",
			sqlText:   "SELECT id, COALESCE(name, 'unknown') FROM current_users UNION ALL SELECT user_id, full_name FROM archived_users",
			selected:  []string{"COALESCE(name, 'unknown')"},
			expectErr: "set query projection expression \"COALESCE(name, 'unknown')\" requires an alias",
		},
		{
			name:      "missing selected column errors",
			sqlText:   "SELECT id, name FROM users",
			selected:  []string{"missing"},
			expectErr: "not found column missing",
		},
	}

	for _, useCase := range useCases {
		t.Run(useCase.name, func(t *testing.T) {
			var view *data.View
			if useCase.groupable {
				view = &data.View{Spec: spec.View{Groupable: &useCase.groupable}}
			}
			actual, err := ApplySelectorProjection(useCase.sqlText, useCase.selected, view)
			if useCase.expectErr != "" {
				if err == nil || err.Error() != useCase.expectErr {
					t.Fatalf("expected error %q, got %v", useCase.expectErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if actual != useCase.expectSQL {
				t.Fatalf("unexpected projected SQL:\nactual: %q\nexpect: %q", actual, useCase.expectSQL)
			}
		})
	}
}

func TestApplySelectorProjectionNullFallback(t *testing.T) {
	name := &data.Column{Name: "Name", Column: "name"}
	name.ConfigureNullability(true, "string")
	view := &data.View{Columns: []*data.Column{{Name: "ID", Column: "id"}, name}}
	tests := []struct {
		name     string
		sqlText  string
		selected []string
		view     *data.View
		want     string
	}{
		{
			name:    "complete projection",
			sqlText: "SELECT id, name FROM users",
			view:    view,
			want:    "SELECT id, COALESCE(name, '') AS name FROM users",
		},
		{
			name:     "selected projection",
			sqlText:  "SELECT id, name FROM users",
			selected: []string{"name"},
			view:     view,
			want:     "SELECT COALESCE(name, '') AS name FROM users",
		},
		{
			name:    "allow nulls",
			sqlText: "SELECT id, name FROM users",
			view: func() *data.View {
				allow := true
				return &data.View{Spec: spec.View{AllowNulls: &allow}, Columns: view.Columns}
			}(),
			want: "SELECT id, name FROM users",
		},
		{
			name:     "set query outer projection",
			sqlText:  "SELECT id, name FROM current_users UNION ALL SELECT user_id AS id, full_name AS name FROM archived_users",
			selected: []string{"name"},
			view:     view,
			want:     "SELECT COALESCE(name, '') AS name FROM (SELECT id, name FROM current_users UNION ALL SELECT user_id AS id, full_name AS name FROM archived_users) AS datly_set",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := ApplySelectorProjection(test.sqlText, test.selected, test.view)
			if err != nil {
				t.Fatalf("ApplySelectorProjection() error = %v", err)
			}
			if actual != test.want {
				t.Fatalf("ApplySelectorProjection() = %q, want %q", actual, test.want)
			}
		})
	}
}

func TestApplySelectorProjectionExpandsWildcardFromCanonicalView(t *testing.T) {
	name := &data.Column{Name: "Name", Column: "users.name"}
	name.ConfigureNullability(true, "string")
	view := &data.View{Columns: []*data.Column{{Name: "ID", Column: "users.id"}, name}}
	tests := []struct {
		name     string
		sqlText  string
		selected []string
		view     *data.View
		want     string
	}{
		{
			name:    "complete wildcard projection",
			sqlText: "SELECT * FROM users;",
			view:    view,
			want:    "SELECT id, COALESCE(name, '') AS name FROM (SELECT * FROM users) AS datly_view",
		},
		{
			name:     "qualified wildcard selected projection",
			sqlText:  "SELECT users.* FROM users",
			selected: []string{"name"},
			view:     view,
			want:     "SELECT COALESCE(name, '') AS name FROM (SELECT users.* FROM users) AS datly_view",
		},
		{
			name:     "allow nulls selected projection",
			sqlText:  "SELECT * FROM users",
			selected: []string{"name"},
			view: func() *data.View {
				allow := true
				return &data.View{Spec: spec.View{AllowNulls: &allow}, Columns: view.Columns}
			}(),
			want: "SELECT name FROM (SELECT * FROM users) AS datly_view",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := ApplySelectorProjection(test.sqlText, test.selected, test.view)
			if err != nil {
				t.Fatalf("ApplySelectorProjection() error = %v", err)
			}
			if actual != test.want {
				t.Fatalf("ApplySelectorProjection() = %q, want %q", actual, test.want)
			}
		})
	}
}

func TestNormalizeProjectionSelection(t *testing.T) {
	actual := normalizeProjectionSelection([]string{" display_name ", "", "displayName", "DISPLAY_NAME", "user.id"})
	expected := []string{"display_name", "displayname", "user.id"}
	if strings.Join(actual, "|") != strings.Join(expected, "|") {
		t.Fatalf("unexpected normalized selection: got %+v, want %+v", actual, expected)
	}
}

func TestProjectionItemNames(t *testing.T) {
	stmt, err := sqlparser.ParseQuery("SELECT users.name AS display_name FROM users")
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	actual := projectionItemNames(stmt.List[0], "users.name AS display_name")
	expected := []string{"display_name"}
	if strings.Join(actual, "|") != strings.Join(expected, "|") {
		t.Fatalf("unexpected projection item names: got %+v, want %+v", actual, expected)
	}
}

func TestProjectionItemSelected_BlankCandidateIgnored(t *testing.T) {
	matched, names := projectionItemSelected(nil, "   ", []string{""})
	if matched || len(names) != 0 {
		t.Fatalf("expected blank projection item to not match, got %v %+v", matched, names)
	}
}
