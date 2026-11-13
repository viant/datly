package reader_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/sqlx/testutil/sqlfault"
	structjson "github.com/viant/structology/encoding/json"
	xresponse "github.com/viant/xdatly/response"
	xstate "github.com/viant/xdatly/state"
)

type selectionModeChild struct {
	ParentID int `sqlx:"parent_id" json:"parentId"`
	Value    int `sqlx:"value" json:"value"`
}
type selectionModeRow struct {
	ID       int                   `sqlx:"id" json:"id"`
	Name     *string               `sqlx:"name" json:"name"`
	Kind     string                `sqlx:"kind" internal:"true" json:"-"`
	Class    string                `sqlx:"-" json:"class"`
	Assigned bool                  `sqlx:"-" json:"assigned"`
	Children []*selectionModeChild `internal:"true" json:"-"`
}

func (r *selectionModeRow) OnFetch(context.Context) error {
	r.Class = "unsupported"
	if r.Kind == "banner" {
		r.Class = "banner"
	}
	return nil
}
func (r *selectionModeRow) OnRelation(context.Context) { r.Assigned = len(r.Children) > 0 }

func TestFieldSelectionModePreservesLifecycleGraphSQLite(t *testing.T) {
	for _, mode := range []spec.FieldSelectionMode{"", spec.FieldSelectionQuery, spec.FieldSelectionOutputOnly} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := t.Context()
			h := sqlite.New(t)
			require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER,name TEXT,kind TEXT,tenant INTEGER)", "CREATE TABLE children(parent_id INTEGER,value INTEGER)", "INSERT INTO parents VALUES(1,'one','banner',1),(2,NULL,'banner',1),(3,'denied','banner',2)", "INSERT INTO children VALUES(1,10),(3,30)"))
			var queries atomic.Int32
			db := h.FaultDB(t, func(_ context.Context, call sqlfault.Call) error {
				if call.Phase == "query" && (strings.Contains(call.SQL, "FROM parents") || strings.Contains(call.SQL, "FROM children")) {
					queries.Add(1)
				}
				return nil
			})
			c := &spec.Component{RootView: &spec.View{Name: "parents", Selector: &spec.Selector{AllowFields: true, FieldSelection: mode}, Source: &spec.ViewSource{SQL: "SELECT id,name,kind FROM parents WHERE tenant=1"}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, View: &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT parent_id,value FROM children"}}, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}}}}}
			typ := reflect.TypeFor[[]*selectionModeRow]()
			plan, err := compiler.Compile(compiler.Input{Component: c, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
			require.NoError(t, err)
			execution, err := reader.NewExecution(reader.Config{Component: c, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: db}})
			require.NoError(t, err)
			read := func(selector *xstate.Selector) ([]*selectionModeRow, []byte, error) {
				call, complete := dexec.ScopeOutputSelection(dexec.CaptureOutputSelection(ctx))
				dexec.BeginOutputSelection(call)
				var selectors xstate.Selectors
				if selector != nil {
					selectors = xstate.Selectors{&xstate.NamedSelector{Name: "parents", Selector: *selector}}
				}
				value, err := execution.Read(call, &struct{}{}, sourceGuardBinder{selectors}, nil)
				if err != nil {
					return nil, nil, err
				}
				complete(value, err)
				filter, available := dexec.CurrentOutputSelection(call, value)
				require.True(t, available)
				encoded, err := structjson.MarshalStandard(value, structjson.WithPathFieldExcluder(filter))
				return value.([]*selectionModeRow), encoded, err
			}
			queries.Store(0)
			baseline, _, err := read(nil)
			require.NoError(t, err)
			require.Len(t, baseline, 2)
			require.Equal(t, "banner", baseline[0].Class)
			require.True(t, baseline[0].Assigned)
			require.False(t, baseline[1].Assigned)
			require.Nil(t, baseline[1].Name)
			baselineQueries := queries.Load()
			require.EqualValues(t, 2, baselineQueries, "one parent and one batched child query")
			for iteration := 0; iteration < 2; iteration++ {
				queries.Store(0)
				rows, encoded, err := read(&xstate.Selector{Fields: []string{"id", "class", "assigned"}})
				require.NoError(t, err)
				if mode == spec.FieldSelectionOutputOnly {
					require.Equal(t, baseline, rows)
					require.JSONEq(t, `[{"id":1,"class":"banner","assigned":true},{"id":2,"class":"banner","assigned":false}]`, string(encoded))
					require.Equal(t, baselineQueries, queries.Load(), "response mask must retain complete graph query count")
				} else {
					require.Equal(t, "unsupported", rows[0].Class)
					require.False(t, rows[0].Assigned)
					require.EqualValues(t, 1, queries.Load(), "compatibility mode prunes child relation")
				}
			}
			// Empty and omitted Fields retain the same internal null/empty semantics.
			rows, _, err := read(&xstate.Selector{Fields: []string{}})
			require.NoError(t, err)
			require.Equal(t, baseline, rows)
			if mode == spec.FieldSelectionOutputOnly {
				// Explicit trusted Columns retain their existing read authority.
				rows, _, err = read(&xstate.Selector{Fields: []string{"id", "class"}, Columns: []string{"id"}})
				require.NoError(t, err)
				require.Equal(t, "unsupported", rows[0].Class)
				require.Nil(t, rows[0].Children)
				// An explicit trusted relation restriction still narrows the graph
				// independently of response Fields, without suppressing scalars.
				originalContext := ctx
				target := dexec.ComponentTarget{Component: c.Key}
				ctx, err = dexec.EnterRelationSelection(ctx, target, dexec.NewRelationSelection(target, "parents"))
				require.NoError(t, err)
				rows, _, err = read(&xstate.Selector{Fields: []string{"id", "class", "assigned"}})
				require.NoError(t, err)
				require.Equal(t, "banner", rows[0].Class)
				require.False(t, rows[0].Assigned)
				require.Nil(t, rows[0].Children)
				ctx = originalContext
			}
			// SQL-mapped internal fields keep their existing source-selection
			// semantics; the external encoder must still hide their values.
			_, hidden, err := read(&xstate.Selector{Fields: []string{"Kind"}})
			require.NoError(t, err)
			require.JSONEq(t, `[{},{}]`, string(hidden))
			for _, name := range []string{"missing"} {
				queries.Store(0)
				_, _, err := read(&xstate.Selector{Fields: []string{name}})
				require.Error(t, err)
				var response *xresponse.Error
				require.True(t, errors.As(err, &response))
				require.Equal(t, 400, response.Code)
				if mode == spec.FieldSelectionOutputOnly {
					require.EqualValues(t, 0, queries.Load(), "unknown field fails before retrieval")
				}
			}
		})
	}
}
