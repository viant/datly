package sql

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"testing"
)

func TestPreparedNamedWildcardComputedOutputs(t *testing.T) {
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{Table: "records"}}, Columns: []*data.Column{
		{Name: "Id", Column: "id"}, {Name: "Status", Column: "status"},
		{Name: "TransitionAt", Column: "transition_at"},
	}}
	for _, source := range []string{
		`SELECT n.* FROM (SELECT q.*, CASE WHEN q.status='done' THEN 'end' ELSE 'start' END AS transition_at FROM records q) n`,
		`SELECT n.* FROM (SELECT q.* FROM (SELECT r.*, COALESCE(r.status,'pending') AS transition_at FROM records r) q WHERE q.id <> '') n`,
		`WITH rows AS (SELECT r.*, COALESCE(r.status,'pending') AS transition_at FROM records r) SELECT n.* FROM rows n`,
		`SELECT n.* FROM (SELECT r.*, COALESCE(o.status,r.status) AS transition_at FROM records r LEFT JOIN other o ON o.id=r.id) n`,
	} {
		t.Run(source, func(t *testing.T) {
			projection := SelectorProjection{SQL: source, View: view}
			columns, err := projection.Columns(nil)
			require.NoError(t, err)
			require.Len(t, columns, 3)
			for _, selected := range [][]string{nil, {"id"}, {"transition_at"}} {
				result, err := projection.Prepare(selected)
				require.NoError(t, err)
				require.Contains(t, result.Render(result.Source), "AS transition_at")
			}
		})
	}
}

func TestPreparedNamedWildcardComputedGuards(t *testing.T) {
	view := &data.View{Spec: spec.View{Source: &spec.ViewSource{Table: "records"}}, Columns: []*data.Column{{Name: "Id", Column: "id"}, {Name: "Derived", Column: "derived"}}}
	for _, source := range []string{
		`SELECT n.* FROM (SELECT r.*, r.id+1 AS unknown FROM records r) n`,
		`SELECT n.* FROM (SELECT r.*, r.id+1 AS derived, COALESCE(r.id,0) AS derived FROM records r) n`,
		`SELECT n.* FROM (SELECT wrong.*, r.id+1 AS derived FROM records r) n`,
		`SELECT n.* FROM (SELECT *, r.id+1 AS derived FROM records r JOIN other o ON o.id=r.id) n`,
		`SELECT n.* FROM (SELECT o.*, r.id+1 AS derived FROM records r JOIN other o ON o.id=r.id) n`,
		`SELECT n.* FROM (SELECT r.* EXCEPT(id), r.id+1 AS derived FROM records r) n`,
		`SELECT n.* FROM (SELECT r.id AS renamed, r.id+1 AS derived FROM records r) n`,
	} {
		t.Run(source, func(t *testing.T) {
			_, err := (SelectorProjection{SQL: source, View: view}).Columns([]string{"id"})
			require.Error(t, err)
		})
	}
	_, err := (SelectorProjection{SQL: `SELECT n.* FROM (SELECT r.*, r.id+1 AS derived FROM records r) n`}).Columns(nil)
	require.Error(t, err)
	for _, table := range []string{"", "unrelated"} {
		view.Spec.Source.Table = table
		_, err := (SelectorProjection{SQL: `SELECT n.* FROM (SELECT r.*, COALESCE(o.id,r.id) AS derived FROM records r LEFT JOIN other o ON o.id=r.id) n`, View: view}).Columns(nil)
		require.Error(t, err, "joined wildcard must not borrow another table contract")
	}
}
