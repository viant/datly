package runtime

import (
	"context"
	"fmt"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/provider/values"
	"github.com/viant/datly/bootstrap"
	fixture "github.com/viant/datly/bootstrap/testdata/selectorcompanion"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	rh "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/datly/sql/reader"
	xh "github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/state"
	"reflect"
	"strings"
	"testing"
)

type companionCountingSource struct {
	source dml.Source
	opens  int
}

func (s *companionCountingSource) Open(ctx context.Context) (xh.Data, error) {
	s.opens++
	return s.source.Open(ctx)
}

func TestSelectorCompanionBufferedDispatcherSQLite(t *testing.T) {
	for _, mode := range []string{"commit", "outer failure", "child binding failure", "overlapping selectors"} {
		t.Run(mode, func(t *testing.T) {
			db := sqlite.New(t)
			ctx := t.Context()
			if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two')", "CREATE TABLE audit(id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			found, err := bootstrap.ReflectSelectedPackages([]string{reflect.TypeFor[fixture.Component]().PkgPath()}, nil)
			if err != nil {
				t.Fatal(err)
			}
			c, err := found.Components[0].Resolve(reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output]())
			if err != nil {
				t.Fatal(err)
			}
			a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output]()})
			if err != nil {
				t.Fatal(err)
			}
			a.Reader.Root.View.Spec.Selector.AllowOrderBy = true
			execution, err := reader.NewExecution(reader.Config{Component: a.Component, InputType: reflect.TypeFor[fixture.Input](), OutputType: reflect.TypeFor[fixture.Output](), Plan: a.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
			if err != nil {
				t.Fatal(err)
			}
			childSource := &companionCountingSource{source: dml.Source{DB: db.DB}}
			child := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[fixture.Output](), Reader: execution, DataSource: childSource}
			target := dexec.ComponentTarget{Component: c.Key, Route: spec.RouteRef{Method: "GET", Path: "/selector-records"}}
			ps := componentSpec("CompanionParent", "POST", "/companion-parent", nil)
			ps.Settings = &spec.Settings{ComponentCallPolicy: "buffered"}
			pa := componentArtifact(t, ps, reflect.TypeFor[struct{}](), reflect.TypeFor[struct{}]())
			commits, completed := 0, 0
			parentSource := &companionCountingSource{source: dml.Source{DB: db.DB, OnCommit: func(context.Context) { commits++ }}}
			parent := &registry.RegisteredComponent{Component: pa.Component, Input: pa.Input, OutputType: reflect.TypeFor[struct{}](), DataSource: parentSource, Handler: rh.HandlerFunc(func(ctx context.Context, inv rh.Invocation) (any, error) {
				raw, _, err := inv.Binder.Lookup(ctx, xh.DataKey)
				if err != nil {
					return nil, err
				}
				data := raw.(xh.Data)
				if err = data.Execute("INSERT INTO audit VALUES(1)"); err != nil {
					return nil, err
				}
				raw, _, err = inv.Binder.Lookup(ctx, dexec.ComponentInvokerKey)
				if err != nil {
					return nil, err
				}
				caller := raw.(dexec.ComponentInvoker)
				call := func(fields string) error {
					page := "1"
					if mode == "child binding failure" {
						page = "bad"
					}
					providers := []locator.Provider{values.New("query", map[string]any{"id": "9", "fields": []string{"id"}, "page": page}), values.New(string(xh.SelectorsKey), map[string]any{"": state.Selectors{&state.NamedSelector{Name: "rows", Selector: state.Selector{Fields: strings.Split(fields, ","), OrderBy: "id DESC"}}}})}
					v, e := caller.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: providers})
					if e != nil {
						return e
					}
					out := v.(*fixture.Output)
					if len(out.Data) != 2 || out.Data[0].ID != 2 || out.Data[1].ID != 1 {
						return fmt.Errorf("named order missing: %+v", out.Data)
					}
					wantLabel := ""
					if fields == "id,label" {
						wantLabel = "two"
					}
					if out.Data[0].Label != wantLabel {
						return fmt.Errorf("projection %q leaked: label=%q", fields, out.Data[0].Label)
					}
					return nil
				}
				if mode == "overlapping selectors" {
					ch := make(chan error, 2)
					for _, f := range []string{"id", "id,label"} {
						go func(f string) { ch <- call(f) }(f)
					}
					var first error
					for i := 0; i < 2; i++ {
						if e := <-ch; e != nil && first == nil {
							first = e
						}
					}
					if first != nil {
						return nil, first
					}
				} else if err := call("id"); err != nil {
					return nil, err
				}
				if parentSource.opens != 1 || childSource.opens != 0 || commits != 0 || completed != 0 {
					return nil, fmt.Errorf("ownership changed: opens=%d/%d commits=%d completed=%d", parentSource.opens, childSource.opens, commits, completed)
				}
				tx, err := dexec.InvocationTransaction(ctx, db.DB)
				if err != nil {
					return nil, err
				}
				var count int
				if tx != nil {
					err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count)
				} else {
					err = db.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit").Scan(&count)
				}
				if err != nil {
					return nil, err
				}
				if count != 0 {
					return nil, fmt.Errorf("parent drained early: %d", count)
				}
				if mode == "outer failure" {
					return nil, fmt.Errorf("outer failure")
				}
				return &struct{}{}, nil
			})}
			rt, err := NewRuntime([]*registry.RegisteredComponent{parent, child})
			if err != nil {
				t.Fatal(err)
			}
			defer rt.Shutdown(ctx)
			_, err = rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: ps.Key, Route: spec.RouteRef{Method: "POST", Path: "/companion-parent"}}, Input: &struct{}{}, Completion: func(xh.Outcome) { completed++ }})
			fail := mode == "outer failure" || mode == "child binding failure"
			if (err != nil) != fail {
				t.Fatalf("mode %s err %v", mode, err)
			}
			var count int
			if e := db.DB.QueryRow("SELECT COUNT(*) FROM audit").Scan(&count); e != nil {
				t.Fatal(e)
			}
			want := 1
			if fail {
				want = 0
			}
			if count != want || commits != want || completed != 1 || parentSource.opens != 1 || childSource.opens != 0 {
				t.Fatalf("count=%d commits=%d completed=%d opens=%d/%d", count, commits, completed, parentSource.opens, childSource.opens)
			}
		})
	}
}
