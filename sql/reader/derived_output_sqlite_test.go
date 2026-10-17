package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime"
	hp "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	structjson "github.com/viant/structology/encoding/json"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

type derivedSelectorChild struct {
	ParentID int    `sqlx:"parent_id"`
	Value    int    `sqlx:"value"`
	Label    string `sqlx:"-" json:"child-label"`
}
type derivedSelectorRow struct {
	ID       int                     `sqlx:"id" json:"id"`
	Name     string                  `sqlx:"name" json:"name"`
	Total    *int                    `sqlx:"-" json:"computed-total"`
	Hidden   int                     `sqlx:"-" json:"-"`
	Children []*derivedSelectorChild `json:"-" internal:"true"`
}
type derivedSelectorOutput struct {
	Data      []*derivedSelectorRow `parameter:"Data,kind=output,in=view" json:"data"`
	Finalized int                   `json:"-"`
}

func (o *derivedSelectorOutput) Finalize(context.Context) error {
	o.Finalized++
	for _, r := range o.Data {
		if len(r.Children) > 0 {
			n := 0
			for _, c := range r.Children {
				n += c.Value
				c.Label = "derived"
			}
			r.Total = &n
		}
	}
	return nil
}

func TestDerivedOutputSelectorsSQLite(t *testing.T) {
	for _, static := range []bool{false, true} {
		label := "ordinary"
		SQL := "SELECT id,name FROM parents"
		if static {
			label = "static"
			SQL = "SELECT p.* FROM (SELECT id,name FROM parents) p"
		}
		for _, mode := range []string{"mapped", "mixed", "derived only", "empty", "unknown", "hidden", "forbidden", "order denied", "empty rows"} {
			t.Run(label+"/"+mode, func(t *testing.T) {
				ctx := context.Background()
				h := sqlite.New(t)
				require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER,name TEXT)", "CREATE TABLE children(parent_id INTEGER,value INTEGER)", "INSERT INTO parents VALUES(1,'one'),(2,'two')", "INSERT INTO children VALUES(1,10),(2,20)"))
				if mode == "empty rows" {
					require.NoError(t, h.ExecStatements(ctx, "DELETE FROM parents"))
				}
				policy := &spec.Selector{AllowFields: mode != "forbidden", AllowOrderBy: true}
				child := &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT parent_id,value FROM children"}}
				component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "test/derived", Name: "Derived"}, Name: "Derived", Routes: []*spec.Route{{Method: "GET", Path: "/derived"}}, RootView: &spec.View{Name: "parents", Selector: policy, Source: &spec.ViewSource{SQL: SQL}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, View: child, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}}}}}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[derivedSelectorOutput]()})
				require.NoError(t, err)
				execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[derivedSelectorOutput](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
				require.NoError(t, err)
				app, err := runtime.NewRuntime([]*runtime.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[derivedSelectorOutput](), Reader: execution}})
				require.NoError(t, err)
				defer app.Shutdown(ctx)
				fields := []string{"computed-total"}
				switch mode {
				case "mapped":
					fields = []string{"id"}
				case "mixed":
					fields = []string{"id", "computed-total", "Children"}
				case "empty":
					fields = nil
				case "unknown":
					fields = []string{"computed-total", "unknown"}
				case "hidden":
					fields = []string{"Hidden"}
				}
				selector := xstate.Selector{Fields: fields}
				if mode == "order denied" {
					selector.OrderBy = "computed-total"
				}
				ctx = dexec.CaptureOutputSelection(ctx)
				value, err := app.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/derived"}}, Providers: []locator.Provider{hp.Static(xhandler.SelectorsKey, xstate.Selectors{&xstate.NamedSelector{Name: "parents", Selector: selector}})}})
				if mode == "unknown" || mode == "hidden" || mode == "forbidden" || mode == "order denied" {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				out := value.(*derivedSelectorOutput)
				require.Equal(t, 1, out.Finalized)
				if mode == "empty rows" {
					require.Empty(t, out.Data)
					return
				}
				require.Len(t, out.Data, 2)
				filter := dexec.SelectedOutputFields(ctx, value)
				encoded, err := structjson.MarshalStandard(out, structjson.WithPathFieldExcluder(filter))
				require.NoError(t, err)
				switch mode {
				case "mapped":
					require.JSONEq(t, `{"data":[{"id":1},{"id":2}]}`, string(encoded))
					require.Nil(t, out.Data[0].Children)
				case "mixed":
					require.JSONEq(t, `{"data":[{"id":1,"computed-total":10},{"id":2,"computed-total":20}]}`, string(encoded))
					require.Len(t, out.Data[0].Children, 1)
				case "derived only":
					require.JSONEq(t, `{"data":[{"computed-total":null},{"computed-total":null}]}`, string(encoded))
					require.Nil(t, out.Data[0].Children)
				}
			})
		}
	}
}

// A derived-only mask borrows the exact unprojected baseline; it must not change
// cardinality even when the authored SQL already owns DISTINCT or aggregation.
func TestDerivedOnlyPreservesAuthoredCardinalityBaseline(t *testing.T) {
	for _, SQL := range []string{"SELECT DISTINCT name FROM parents", "SELECT COUNT(*) AS id FROM parents", "SELECT name,COUNT(*) AS id FROM parents GROUP BY name"} {
		t.Run(SQL, func(t *testing.T) {
			h := sqlite.New(t)
			require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE parents(id INTEGER,name TEXT)", "INSERT INTO parents VALUES(1,'same'),(2,'same')"))
			c := &spec.Component{RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: SQL}, Selector: &spec.Selector{AllowFields: true}}}
			plan, err := compiler.Compile(compiler.Input{Component: c, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[[]*derivedSelectorRow](), DirectViewType: reflect.TypeFor[[]*derivedSelectorRow]()})
			require.NoError(t, err)
			execution, err := reader.NewExecution(reader.Config{Component: c, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[[]*derivedSelectorRow](), Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
			require.NoError(t, err)
			baseline, err := execution.ReadResult(t.Context(), &struct{}{}, sourceGuardBinder{}, nil)
			require.NoError(t, err)
			selected, err := execution.ReadResult(t.Context(), &struct{}{}, sourceGuardBinder{xstate.Selectors{&xstate.NamedSelector{Name: "parents", Selector: xstate.Selector{Fields: []string{"computed-total"}}}}}, nil)
			require.NoError(t, err)
			require.Len(t, selected.Data.([]*derivedSelectorRow), len(baseline.Data.([]*derivedSelectorRow)))
			require.Len(t, selected.Data.([]*derivedSelectorRow), 1)
		})
	}
}
