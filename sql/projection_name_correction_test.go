package sql

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"testing"
)

func TestProjectionNamesPreserveIdentifierParts(t *testing.T) {
	names := []string{"b.id", "b_id", "bid", `"b.id"`, `"b"."i.d"`, "b.i_d", "b.id_"}
	for _, from := range names {
		for _, to := range names {
			require.Equal(t, from == to, (ProjectionNames{from}).Matches(to), "%s -> %s", from, to)
		}
	}
	for _, pair := range [][2]string{{"B.ID", `"b".[id]`}, {"B_ID", "b_id"}, {"BID", "bid"}, {`"odd.name"`, `[odd.name]`}, {`"b"."odd.name"`, "`b`.`odd.name`"}, {`"a""b.c"`, `[a"b.c]`}} {
		require.True(t, (ProjectionNames{pair[0]}).Matches(pair[1]), pair)
	}
	for _, name := range []string{`"odd.name"`, `[odd.name]`, "`odd.name`"} {
		got := projectionItemNames(nil, name)
		require.Len(t, got, 1)
		require.False(t, ProjectionNames(got).Matches("name"))
		require.False(t, ProjectionNames(got).Matches("odd.name"))
	}
	namesForQualified := projectionItemNames(nil, `"b"."odd.name"`)
	require.True(t, ProjectionNames(namesForQualified).Matches(`[odd.name]`))
	require.False(t, ProjectionNames(namesForQualified).Matches("name"))
	require.False(t, ProjectionNames(namesForQualified).Matches("b.odd.name"))
}

func TestProjectionMappingsRequireSourceProof(t *testing.T) {
	for _, tc := range []struct {
		name, source, selected string
		columns                []*data.Column
		mappings               []*spec.Column
		reject                 bool
	}{
		{name: "inferred field rejected", source: "SELECT b.* FROM users b", selected: "bid", columns: []*data.Column{{Name: "BID", Column: "b.id"}}, reject: true},
		{name: "inferred unqualified field rejected", source: "SELECT * FROM users", selected: "bid", columns: []*data.Column{{Name: "BID", Column: "id"}}, reject: true},
		{name: "inferred canonical metadata is not mapping authority", source: "SELECT b_id FROM users", selected: "bid", mappings: []*spec.Column{{Name: "bid", NameInferred: true, Source: "b_id"}}, reject: true},
		{name: "canonical field mapping", source: "SELECT b.id FROM users b", selected: "bid", mappings: []*spec.Column{{Name: "bid", Source: "id"}}},
		{name: "qualified canonical field mapping", source: "SELECT bid FROM users", selected: "b.id", mappings: []*spec.Column{{Name: "b.id", Source: "bid"}}},
		{name: "row tag is not selector alias mapping", source: "SELECT b.id FROM users b", selected: "BID", columns: []*data.Column{{Name: "BID", Column: "b.id", Tag: `sqlx:"b.id"`}}, reject: true},
		{name: "unrelated tag cannot widen", source: "SELECT b_id FROM users", selected: "BID", columns: []*data.Column{{Name: "BID", Column: "b.id", Tag: `sqlx:"b.id"`}}, reject: true},
		{name: "mapping cannot flatten target", source: "SELECT bid FROM users", selected: "display", mappings: []*spec.Column{{Name: "display", Source: "b.id"}}, reject: true},
		{name: "mapping ambiguous target", source: "SELECT a.id,b.id FROM users a JOIN users b ON a.id=b.id", selected: "display", mappings: []*spec.Column{{Name: "display", Source: "id"}}, reject: true},
		{name: "mapping no chaining", source: "SELECT id FROM users", selected: "display", mappings: []*spec.Column{{Name: "intermediate", Source: "id"}, {Name: "display", Source: "intermediate"}}, reject: true},
		{name: "EXCEPT rejects qualifier as output alias", source: "SELECT b.* EXCEPT(b.id) FROM users b", selected: "id", columns: []*data.Column{{Name: "ID", Column: "b.id"}}, reject: true},
		{name: "EXCEPT blocks mapping", source: "SELECT b.* EXCEPT(id) FROM users b", selected: "bid", columns: []*data.Column{{Name: "ID", Column: "b.id"}, {Name: "Name", Column: "b.name"}}, mappings: []*spec.Column{{Name: "bid", Source: "id"}}, reject: true},
		{name: "mixed CTE mapping", source: "WITH b AS (SELECT id,name FROM users) SELECT b.*,1 AS extra FROM b", selected: "bid", mappings: []*spec.Column{{Name: "bid", Source: "id"}}},
		{name: "CTE dot alias stays quoted", source: `WITH b AS (SELECT id AS "b.id" FROM users) SELECT b.* FROM b`, selected: `"b.id"`},
		{name: "CTE dot alias not qualified", source: `WITH b AS (SELECT id AS "b.id" FROM users) SELECT b.* FROM b`, selected: "b.id", reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view := &data.View{Spec: spec.View{Columns: tc.mappings}, Columns: tc.columns}
			p := SelectorProjection{SQL: tc.source, View: view}
			_, err := p.Columns([]string{tc.selected})
			if tc.reject {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			_, err = ApplySelectorProjection(tc.source, []string{tc.selected}, view)
			require.NoError(t, err)
		})
	}
}

func TestComputedOutputLabelsDoNotCreateSourceAlternatives(t *testing.T) {
	p := SelectorProjection{SQL: `SELECT 'foo' FROM users`}
	_, err := p.Columns([]string{"foo"})
	require.Error(t, err)
	_, err = p.Columns([]string{`"'foo'"`})
	require.NoError(t, err)
	p.SQL = `SELECT COUNT(id),COUNT(id) AS "COUNT(id)" FROM users`
	_, err = p.Columns(nil)
	require.ErrorContains(t, err, "assign distinct SQL aliases")
}
