package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	xh "github.com/viant/xdatly/handler"
)

type outputInvokerInput struct {
	Name string `parameter:"Name,kind=query,in=name,required"`
}
type outputInvokerRow struct {
	ID    int    `sqlx:"id"`
	Label string `sqlx:"label"`
}
type outputInvokerOutput struct {
	Invoker xh.ComponentInvoker `parameter:"Invoker,kind=component_invoker,required" json:"-"`
	Data    []*outputInvokerRow `parameter:"Data,kind=output,in=view" view:"Rows" sql:"SELECT id,label FROM records"`
	Child   string              `json:"child"`
}

var outputInvokerVeto = errors.New("output finalizer veto")

func (o *outputInvokerOutput) Finalize(ctx context.Context, failure error) error {
	if failure != nil {
		return failure
	}
	if o.Invoker == nil {
		return errors.New("output invoker unavailable")
	}
	value, err := o.Invoker.Invoke(ctx, xh.ComponentRequest{Target: xh.ComponentTarget{Scope: "example.com/components", Name: "OutputChild", Method: "PATCH", Path: "/output-child"}})
	if err != nil {
		return err
	}
	o.Child = value.(string)
	if len(o.Data) > 0 && o.Data[0].Label == "veto" {
		return outputInvokerVeto
	}
	return nil
}

func TestOutputInvokerNativeReaderScopedBufferedSQLite(t *testing.T) {
	for _, veto := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "rollback"}[veto], func(t *testing.T) {
			db := sqlite.New(t)
			label := "ok"
			if veto {
				label = "veto"
			}
			require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "CREATE TABLE audit(name TEXT)"))
			_, err := db.DB.ExecContext(t.Context(), "INSERT INTO records VALUES(1,?)", label)
			require.NoError(t, err)
			parentSpec := componentSpec("OutputParent", "GET", "/output-parent", nil)
			parentSpec.Settings = &spec.Settings{ComponentCallPolicy: "buffered"}
			a := componentArtifact(t, parentSpec, reflect.TypeFor[outputInvokerInput](), reflect.TypeFor[outputInvokerOutput]())
			reader, err := a.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
			require.NoError(t, err)
			commits := 0
			source := dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}
			childSource := dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}
			childArtifact := componentArtifact(t, componentSpec("OutputChild", "PATCH", "/output-child", nil), reflect.TypeFor[outputInvokerInput](), reflect.TypeFor[struct{}]())
			childCalls := 0
			child := &registry.RegisteredComponent{Component: childArtifact.Component, Input: childArtifact.Input, OutputType: reflect.TypeFor[struct{}](), DataSource: childSource, Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				childCalls++
				input := inv.Input.(*outputInvokerInput)
				raw, found, err := inv.Binder.Lookup(ctx, xh.DMLKey)
				if err != nil {
					return nil, err
				}
				if !found {
					return nil, errors.New("missing shared DML")
				}
				if err = raw.(xh.DML).Execute("INSERT INTO audit(name) VALUES(?)", input.Name); err != nil {
					return nil, err
				}
				return input.Name, nil
			})}
			parent := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[outputInvokerOutput](), Reader: reader, DataSource: source}
			rt, err := NewRuntime([]*registry.RegisteredComponent{parent, child})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, rt.Shutdown(context.Background())) })

			_, err = executeTestRoute(t, rt, t.Context(), testharness.NewRequest("GET", "/output-parent"))
			require.Error(t, err)
			require.Zero(t, childCalls, "early input error must not compose children")
			for _, name := range []string{"alpha", "beta"} {
				value, err := executeTestRoute(t, rt, t.Context(), testharness.NewRequest("GET", "/output-parent?name="+name+"&Invoker=forged"))
				if veto {
					require.ErrorIs(t, err, outputInvokerVeto)
				} else {
					require.NoError(t, err)
					require.Equal(t, name, value.(*outputInvokerOutput).Child)
				}
			}
			require.Equal(t, 2, childCalls)
			var count int
			require.NoError(t, db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&count))
			if veto {
				require.Zero(t, count)
				require.Zero(t, commits)
			} else {
				require.Equal(t, 2, count)
				require.Equal(t, 2, commits)
			}
		})
	}
}
