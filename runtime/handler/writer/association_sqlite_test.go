package writer

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp/tool"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

type associationParent struct {
	ID       *int              `sqlx:"id,primaryKey=true"`
	Name     *string           `sqlx:"name"`
	Children []*sqPlainChild   `view:"Children,table=children" on:"ID=ParentID" association:"collection=labels,member=Label"`
	Has      *sqPlainParentHas `setMarker:"true" sqlx:"-" json:"-"`
}
type associationInput struct {
	Rows            []*associationParent `parameter:"Rows,kind=body,in=data" view:"Rows,table=parents"`
	CurrentRows     []*associationParent `parameter:"CurrentRows,kind=view" view:"CurrentRows,table=parents"`
	CurrentChildren []*sqPlainChild      `parameter:"CurrentChildren,kind=view" view:"CurrentChildren,table=children"`
}
type associationOutput struct {
	Data []*associationParent `parameter:"Data,kind=output,in=body"`
}

func compileAssociationTool(t *testing.T, c *spec.Component) *tool.Plan {
	t.Helper()
	input := reflect.TypeFor[associationInput]()
	injector, err := bindly.NewInjector()
	require.NoError(t, err)
	bindings := []bindly.BindingSpec{{Path: "Rows", Location: state.Location{Kind: "body", In: "data"}}}
	bound, err := injector.CompilePlan(input, bindings...)
	require.NoError(t, err)
	projection, err := bound.Projection()
	require.NoError(t, err)
	route := spec.RouteRef{Method: "PATCH", Path: "/parents"}
	contract, err := registry.NewInputContract(input, projection, registry.RouteInput{Route: route, Plan: bound, Bindings: bindings})
	require.NoError(t, err)
	selected, ok := contract.ForRoute(route)
	require.True(t, ok)
	plan, err := tool.NewCompiler(tool.WithStrictArguments(true)).Compile(tool.Input{
		Component: c.Key, Contract: selected, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "edit_links"},
	})
	require.NoError(t, err)
	return plan
}

func TestAssociationDeltaScopedSQLite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		trigger string
		fail    bool
	}{
		{"swap and replay", "", false},
		{"rollback when removal fails", "CREATE TRIGGER stop_unlink BEFORE DELETE ON children BEGIN SELECT RAISE(ABORT,'unlink denied'); END", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			db := sqlite.New(t)
			err := db.ExecStatements(ctx, sqSchema, "INSERT INTO parents(id,name) VALUES(1,'one'),(2,'two')", "INSERT INTO children(id,parent_id,label) VALUES(42,1,'old'),(43,1,'keep'),(44,2,'old')", tc.trigger)
			if err != nil {
				t.Fatal(err)
			}
			c := sqComponent("association", "patch", "")
			c.Routes = []*spec.Route{{Method: "PATCH", Path: "/parents"}}
			plan := compileAssociationTool(t, c)
			count := sqObserveWrites(t, ctx, db)
			for attempt := 0; attempt < 2; attempt++ {
				in := &associationInput{}
				sqLoadCurrent(t, ctx, db, &in.CurrentRows, &in.CurrentChildren)
				args := map[string]any{"data": []any{map[string]any{
					"id": 1, "labelsToBeAdded": []any{"new", "keep"}, "labelsToBeDeleted": []any{"old", "absent"},
				}}}
				request, scope, err := plan.AssociationScope(ctx, args)
				require.NoError(t, err)
				value, found, err := scope.Body().Locate(nil).Value(request, reflect.TypeOf(in.Rows), "data")
				scope.Close()
				require.NoError(t, err)
				require.True(t, found)
				in.Rows = value.([]*associationParent)
				writesBefore := count["children"].Load()
				_, err = runSQLiteWriter(t, request, db, c, in, &associationOutput{}, "patch")
				if tc.fail && (err == nil || !strings.Contains(err.Error(), "unlink denied")) {
					t.Fatalf("rollback: %v", err)
				}
				if !tc.fail && err != nil {
					t.Fatal(err)
				}
				var actual string
				err = db.DB.QueryRowContext(ctx, "SELECT group_concat(parent_id || '/' || label, ',') FROM (SELECT parent_id,label FROM children ORDER BY parent_id,label)").Scan(&actual)
				if err != nil {
					t.Fatal(err)
				}
				want := "1/keep,1/new,2/old"
				if tc.fail {
					want = "1/keep,1/old,2/old"
				}
				if actual != want {
					t.Fatalf("attempt %d: %s, want %s", attempt, actual, want)
				}
				if count["parents"].Load() != 0 {
					t.Fatal("phantom parent update")
				}
				if attempt == 1 && !tc.fail && count["children"].Load() != writesBefore {
					t.Fatal("replaying named associations wrote rows")
				}
			}
		})
	}
}
