package writer

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp/tool"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

func scopedAssociationInput(t *testing.T, plan *tool.Plan, db *sqlite.Harness, rows ...any) (context.Context, *associationInput) {
	t.Helper()
	request, scope, err := plan.AssociationScope(t.Context(), map[string]any{"data": rows})
	require.NoError(t, err)
	defer scope.Close()
	in := &associationInput{}
	body, found, err := scope.Body().Locate(nil).Value(request, reflect.TypeOf(in.Rows), "data")
	require.NoError(t, err)
	require.True(t, found)
	in.Rows = body.([]*associationParent)
	sqLoadCurrent(t, request, db, &in.CurrentRows, &in.CurrentChildren)
	return request, in
}

func TestAssociationDeltaAmbiguousAndStaleOwnerSQLite(t *testing.T) {
	for _, tc := range []struct {
		name      string
		batch     []any
		change    string
		ambiguous bool
	}{
		{"delta followed by ordinary occurrence", []any{map[string]any{"id": 1, "labelsToBeDeleted": []any{"a"}}, map[string]any{"id": 1, "children": []any{}}}, "", true},
		{"ordinary followed by delta occurrence", []any{map[string]any{"id": 1, "children": []any{}}, map[string]any{"id": 1, "labelsToBeAdded": []any{"b"}}}, "", true},
		{"reparent since Current read", []any{map[string]any{"id": 1, "labelsToBeAdded": []any{"b"}, "labelsToBeDeleted": []any{"a"}}}, "UPDATE children SET parent_id=2 WHERE id=70", false},
		{"different member since Current read", []any{map[string]any{"id": 1, "labelsToBeAdded": []any{"b"}, "labelsToBeDeleted": []any{"a"}}}, "UPDATE children SET label='replacement' WHERE id=70", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(t.Context(), sqSchema, "INSERT INTO parents(id,name) VALUES(1,'one'),(2,'two')", "INSERT INTO children(id,parent_id,label) VALUES(70,1,'a')"))
			c := sqComponent("association", "patch", "")
			c.Routes = []*spec.Route{{Method: "PATCH", Path: "/parents"}}
			plan := compileAssociationTool(t, c)
			// MCP rejects the native collection before binding. Still exercise the
			// writer guard if another internal source supplies that collection.
			if tc.ambiguous {
				_, scope, err := plan.AssociationScope(t.Context(), map[string]any{"data": tc.batch})
				require.ErrorContains(t, err, "children")
				require.Nil(t, scope)
			}
			var nativeRows []int
			for i, item := range tc.batch {
				row := item.(map[string]any)
				if _, ok := row["children"]; ok {
					delete(row, "children")
					nativeRows = append(nativeRows, i)
				}
			}
			ctx, in := scopedAssociationInput(t, plan, db, tc.batch...)
			for _, i := range nativeRows {
				in.Rows[i].Children = []*sqPlainChild{}
				in.Rows[i].Has.Children = true
			}
			require.NoError(t, db.ExecStatements(ctx, tc.change))
			_, err := runSQLiteWriter(t, ctx, db, c, in, &associationOutput{}, "patch")
			if tc.ambiguous {
				require.ErrorContains(t, err, "repeated parent identity")
			} else {
				var conflict *xhandler.Conflict
				require.ErrorAs(t, err, &conflict)
			}
			var count, original int
			require.NoError(t, db.DB.QueryRowContext(ctx, "SELECT COUNT(*), SUM(id=70) FROM children").Scan(&count, &original))
			require.Equal(t, 1, count)
			require.Equal(t, 1, original)
		})
	}
}

func TestAssociationDeltaParentOccurrenceWithoutCollectionSQLite(t *testing.T) {
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), sqSchema, "INSERT INTO parents(id,name) VALUES(1,'one')", "INSERT INTO children(id,parent_id,label) VALUES(70,1,'a')"))
	c := sqComponent("association", "patch", "")
	c.Routes = []*spec.Route{{Method: "PATCH", Path: "/parents"}}
	ctx, in := scopedAssociationInput(t, compileAssociationTool(t, c), db, map[string]any{"id": 1}, map[string]any{"id": 1, "labelsToBeDeleted": []any{"a"}})
	_, err := runSQLiteWriter(t, ctx, db, c, in, &associationOutput{}, "patch")
	require.NoError(t, err)
	var count int
	require.NoError(t, db.DB.QueryRowContext(ctx, "SELECT count(*) FROM children").Scan(&count))
	require.Zero(t, count)
}

func TestPartitionAssociationRowsFromBoundMCPRequest(t *testing.T) {
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), sqSchema))
	component := sqComponent("association", "patch", "")
	component.Routes = []*spec.Route{{Method: "PATCH", Path: "/parents"}}
	ctx, in := scopedAssociationInput(t, compileAssociationTool(t, component), db,
		map[string]any{"id": 17, "labelsToBeAdded": []any{"new"}, "labelsToBeDeleted": []any{"old"}})
	target := xhandler.ComponentTarget{Scope: component.Key.Scope, Name: component.Key.Name, Method: "PATCH", Path: "/parents"}
	delta, ok := xhandler.LookupAssociationDelta(ctx, target, reflect.TypeFor[associationParent](), "Children", "Rows[0].Children")
	require.True(t, ok)
	rows := in.Rows[0].Children
	member := func(row *sqPlainChild) string { return *row.Label }
	added, deleted, err := PartitionAssociationRows(delta, rows, member)
	require.NoError(t, err)
	require.Equal(t, "new", member(added[0]))
	require.Equal(t, "old", member(deleted[0]))
	for _, invalid := range [][]*sqPlainChild{rows[:1], {nil, rows[1]}, {rows[1], rows[0]}, {rows[0], rows[0]}} {
		added, deleted, err = PartitionAssociationRows(delta, invalid, member)
		require.Error(t, err)
		require.Nil(t, added)
		require.Nil(t, deleted)
	}
}

func TestAssociationAuxiliaryRejectsOverlappingOwnerRequests(t *testing.T) {
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), sqSchema, "INSERT INTO parents(id,name) VALUES(17,'one')", "INSERT INTO children(id,parent_id,label) VALUES(70,17,'keep')"))
	component := sqComponent("association", "patch", "")
	component.RootView.Relations[0].View.Auxiliary = true
	component.Routes = []*spec.Route{{Method: "PATCH", Path: "/parents"}}
	plan := compileAssociationTool(t, component)
	ctx, in := scopedAssociationInput(t, plan, db,
		map[string]any{"id": 17, "labelsToBeDeleted": []any{"keep"}},
		map[string]any{"id": 17})
	in.Rows[1].Children = []*sqPlainChild{}
	in.Rows[1].Has.Children = true
	_, err := runSQLiteWriter(t, ctx, db, component, in, &associationOutput{}, "patch")
	require.ErrorContains(t, err, "repeated parent identity")
	var count int
	require.NoError(t, db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM children WHERE id=70 AND parent_id=17").Scan(&count))
	require.Equal(t, 1, count)
}
