package sql

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"testing"
)

func TestSelectorSourceProjection(t *testing.T) {
	for _, tc := range []struct {
		name, source string
		metadata     []*data.Column
		fields       []string
		want         []string
		reject       string
	}{
		{name: "named physical wildcard", source: "SELECT n.* FROM (SELECT r.* FROM records r) n", metadata: []*data.Column{{Name: "id"}}, fields: []string{"id"}, want: []string{"id"}},
		{name: "named schema wildcard", source: "SELECT n.* FROM (SELECT records.* FROM main.records) n", metadata: []*data.Column{{Name: "id"}}, fields: []string{"id"}, want: []string{"id"}},
		{name: "named wrong inner alias", source: "SELECT n.* FROM (SELECT wrong.* FROM records r) n", metadata: []*data.Column{{Name: "id"}}, fields: []string{"id"}, reject: "unresolved"},
		{name: "named joined wildcard cannot borrow row", source: "SELECT n.* FROM (SELECT a.* FROM records a JOIN other b ON a.id=b.id) n", metadata: []*data.Column{{Name: "id"}}, fields: []string{"id"}, reject: "prepared columns"},
		{name: "mixed physical", source: "SELECT u.*, 7 AS extra FROM users u", metadata: []*data.Column{{Name: "id"}, {Name: "name"}, {Name: "extra"}}, fields: []string{"extra", "name"}, want: []string{"u.name", "extra"}},
		{name: "mixed derived", source: "SELECT u.*, 7 AS extra FROM (SELECT id,name FROM users) u", fields: []string{"extra", "name"}, want: []string{"u.name", "extra"}},
		{name: "derived rejects inferred Go alias", source: "SELECT u.* FROM (SELECT id FROM users) u", metadata: []*data.Column{{Name: "Key", Column: "id"}}, fields: []string{"Key"}, reject: "not found column"},
		{name: "derived rejects unrelated metadata alias", source: "SELECT u.* FROM (SELECT id FROM users) u", metadata: []*data.Column{{Name: "Secret", Column: "secret"}}, fields: []string{"Secret"}, reject: "not found column"},
		{name: "derived ignores stale metadata", source: "SELECT u.* FROM (SELECT id FROM users) u", metadata: []*data.Column{{Name: "id"}, {Name: "secret"}}, fields: []string{"secret"}, reject: "not found column"},
		{name: "CTE", source: "WITH u AS (SELECT id,name FROM users) SELECT u.*, 7 AS extra FROM u", fields: []string{"extra", "id"}, want: []string{"u.id", "extra"}},
		{name: "nested CTE", source: "WITH u AS (SELECT id FROM users), v AS (SELECT u.* FROM u) SELECT v.* FROM v", fields: []string{"id"}, want: []string{"id"}},
		{name: "joined derived ambiguous", source: "SELECT a.*,b.* FROM (SELECT id FROM users) a JOIN (SELECT id FROM users) b ON a.id=b.id", fields: []string{"id"}, reject: "duplicate output column"},
		{name: "joined derived qualified", source: "SELECT a.*,b.* FROM (SELECT id FROM users) a JOIN (SELECT id FROM users) b ON a.id=b.id", fields: []string{"b.id"}, reject: "duplicate output column"},
		{name: "joined physical qualified", source: "SELECT a.*,b.* FROM users a JOIN users b ON a.id=b.id", metadata: []*data.Column{{Name: "leftID", Column: "a.id"}, {Name: "rightID", Column: "b.id"}}, fields: []string{"b.id"}, reject: "duplicate output column"},
		{name: "joined physical unresolved", source: "SELECT a.*,b.* FROM users a JOIN users b ON a.id=b.id", metadata: []*data.Column{{Name: "id"}}, fields: []string{"id"}, reject: "qualified prepared"},
		{name: "mixed EXCEPT", source: "SELECT u.* EXCEPT(secret), 7 AS extra FROM users u", metadata: []*data.Column{{Name: "id"}, {Name: "secret"}, {Name: "extra"}}, fields: []string{"secret"}, reject: "not found column"},
		{name: "qualified EXCEPT", source: "SELECT u.* EXCEPT(secret) FROM (SELECT id,secret FROM users) u", fields: []string{"secret"}, reject: "not found column"},
		{name: "wrong qualifier", source: "SELECT v.* FROM users u", metadata: []*data.Column{{Name: "id"}}, fields: []string{"id"}, reject: "unresolved"},
		{name: "unknown physical", source: "SELECT u.*,7 AS extra FROM users u", fields: []string{"extra"}, reject: "prepared columns"},
		{name: "explicit ambiguous", source: "SELECT a.id,b.id FROM users a JOIN users b ON a.id=b.id", fields: []string{"id"}, reject: "duplicate output column"},
		{name: "native vocabulary", source: "SELECT u.name AS display_name FROM users u", fields: []string{"Display_Name"}, want: []string{"display_name"}},
		{name: "unbalanced source", source: "SELECT id FROM (users", fields: []string{"id"}, reject: "unresolved"},
		{name: "CTE quoted alias", source: "WITH u AS (SELECT name AS `display_name` FROM users) SELECT u.* FROM u", fields: []string{"Display_Name"}, want: []string{"`display_name`"}},
		{name: "derived quoted alias", source: "SELECT u.* FROM (SELECT name AS `display_name` FROM users) u", fields: []string{"Display_Name"}, want: []string{"`display_name`"}},
		{name: "quoted alias", source: "SELECT u.name AS `display_name` FROM users u", fields: []string{"Display_Name"}, want: []string{"`display_name`"}},
		{name: "sole wildcard request order", source: "SELECT * FROM users", metadata: []*data.Column{{Name: "id"}, {Name: "name"}}, fields: []string{"name", "id"}, want: []string{"name", "id"}},
		{name: "explicit source order", source: "SELECT id,name FROM users", fields: []string{"name", "id"}, want: []string{"id", "name"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := SelectorProjection{SQL: tc.source, View: &data.View{Columns: tc.metadata}}
			got, err := p.Columns(tc.fields)
			if tc.reject != "" {
				require.ErrorContains(t, err, tc.reject)
				_, err = ApplySelectorProjection(tc.source, tc.fields, p.View)
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			var names []string
			for _, c := range got {
				names = append(names, c.OrderExpression())
			}
			require.Equal(t, tc.want, names)
			_, err = ApplySelectorProjection(tc.source, tc.fields, p.View)
			require.NoError(t, err)
		})
	}
}

func TestNullableDerivedProjectionDoesNotAdoptStaleColumns(t *testing.T) {
	view := &data.View{Columns: []*data.Column{{Name: "id", Column: "id", Nullable: true, NullFallback: "0"}, {Name: "secret", Column: "secret", Nullable: true, NullFallback: "''"}}}
	SQL, err := ApplySelectorProjection("SELECT named.* FROM (SELECT id FROM records) named", nil, view)
	require.NoError(t, err)
	require.NotContains(t, SQL, "secret")
	require.Contains(t, SQL, "COALESCE(id, 0)")
}
