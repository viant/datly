package reader_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/sqlx/testutil/sqlfault"
	xexec "github.com/viant/xdatly/exec"
)

type policyLifecycleKey struct{}
type policyLifecycleControl struct {
	started   chan struct{}
	release   chan struct{}
	panicRead bool
}
type policyLifecycleRow struct {
	ID int `sqlx:"id"`
}

func (*policyLifecycleRow) OnFetch(ctx context.Context) error {
	c, _ := ctx.Value(policyLifecycleKey{}).(*policyLifecycleControl)
	if c == nil {
		return nil
	}
	if c.panicRead {
		panic("private row panic")
	}
	c.started <- struct{}{}
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type policyLifecycleOutput struct {
	Rows []*policyLifecycleRow `parameter:"Rows,kind=output,in=view" view:"records" sql:"SELECT id FROM records"`
}

type policyChildLifecycleRow struct {
	ID       int `sqlx:"id"`
	ParentID int `sqlx:"parent_id"`
}

func (*policyChildLifecycleRow) OnFetch(ctx context.Context) error {
	control := ctx.Value(policyLifecycleKey{}).(*policyLifecycleControl)
	control.started <- struct{}{}
	select {
	case <-control.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type policyChildLifecycleParent struct {
	ID       int                        `sqlx:"id"`
	Children []*policyChildLifecycleRow `sqlx:"-" view:"children" on:"ID:id=ParentID:parent_id" sql:"SELECT id,parent_id FROM children WHERE $COLUMN_IN"`
}
type policyChildLifecycleOutput struct {
	Rows []*policyChildLifecycleParent `parameter:"Rows,kind=output,in=view" view:"records" sql:"SELECT id FROM records"`
}

func TestObservationSourceOwnerBlockedChildCountAndCancellationSQLite(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER)", "CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO records VALUES(1)", "INSERT INTO children VALUES(10,1)"))
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "module.child", Name: "Records"}, Name: "Records", Routes: []*spec.Route{{Method: "GET", Path: "/records"}}}
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[policyChildLifecycleOutput]()})
	require.NoError(t, err)
	e, err := reader.NewExecution(reader.Config{Component: a.Component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[policyChildLifecycleOutput](), Plan: a.Reader, SQL: &dsql.SQLComponent{DB: h.DB}})
	require.NoError(t, err)
	policy := &observability.Policy{}
	for _, target := range e.ObservationTargets() {
		id, err := target.View.Identity()
		require.NoError(t, err)
		policy.Views = append(policy.Views, observability.ViewObservation{Component: component.Key, ViewIdentity: id, Diagnostic: target.View.Name + "#", Operation: &observability.OperationDescriptor{Name: "platform." + target.View.Name, Location: "platform", Description: target.View.Name + " performance", Provider: observability.Source11}})
	}
	app, err := runtime.NewRuntime([]*runtime.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[policyChildLifecycleOutput](), Reader: e}}, runtime.WithObservability(runtime.ObservabilityConfig{Policy: policy}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, app.Shutdown(context.Background())) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	control := &policyLifecycleControl{started: make(chan struct{}, 1), release: make(chan struct{})}
	ctx = context.WithValue(ctx, policyLifecycleKey{}, control)
	done := make(chan error, 1)
	go func() { done <- policyInvoke(ctx, app, a) }()
	select {
	case <-control.started:
	case <-time.After(3 * time.Second):
		t.Fatal("real child SQL did not block")
	}
	owner := app.Observability().Recorder
	root, child := owner.Values("platform.records"), owner.Values("platform.children")
	if root["Pending"] != 2 || root["Success"] != 1 || child["Success"] != 0 || owner.Cumulative("platform.records", "count") != 1 || owner.Cumulative("platform.children", "count") != 1 {
		t.Errorf("live root/child lifecycle root=%v child=%v counts=%d/%d", root, child, owner.Cumulative("platform.records", "count"), owner.Cumulative("platform.children", "count"))
	}
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("child cancellation did not finish")
	}
	root, child = owner.Values("platform.records"), owner.Values("platform.children")
	require.EqualValues(t, 1, root["Success"])
	require.Zero(t, root["Error"])
	require.Zero(t, root["Pending"])
	require.EqualValues(t, 1, child["Error"])
	require.Zero(t, child["Success"])
	require.Zero(t, child["Pending"])
	require.EqualValues(t, 1, owner.Cumulative("platform.records", "count"))
	require.EqualValues(t, 1, owner.Cumulative("platform.children", "count"))
}

func policyLifecycleRuntime(t *testing.T, db *sql.DB, observation ...runtime.ObservabilityConfig) (*runtime.Runtime, *bootstrap.Artifact) {
	t.Helper()
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "module.lifecycle", Name: "Records"}, Name: "Records", Routes: []*spec.Route{{Method: "GET", Path: "/records"}}}
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[policyLifecycleOutput]()})
	require.NoError(t, err)
	e, err := reader.NewExecution(reader.Config{Component: a.Component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[policyLifecycleOutput](), Plan: a.Reader, SQL: &dsql.SQLComponent{DB: db}})
	require.NoError(t, err)
	id, err := a.Reader.Root.View.Spec.Identity()
	require.NoError(t, err)
	p := &observability.Policy{Views: []observability.ViewObservation{{Component: a.Component.Key, ViewIdentity: id, Diagnostic: "records#", Operation: &observability.OperationDescriptor{Name: "platform.records", Location: "platform", Description: "records performance", Provider: observability.Source11}}}}
	config := runtime.ObservabilityConfig{Policy: p}
	if len(observation) > 0 {
		config = observation[0]
		config.Policy = p
	}
	app, err := runtime.NewRuntime([]*runtime.RegisteredComponent{{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[policyLifecycleOutput](), Reader: e}}, runtime.WithObservability(config))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, app.Shutdown(context.Background())) })
	return app, a
}

func policyInvoke(ctx context.Context, app *runtime.Runtime, a *bootstrap.Artifact) error {
	_, err := app.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: a.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records"}}, Input: &struct{}{}})
	return err
}

func sourceCatalogCount(t *testing.T, owner *observability.Recorder) int64 {
	t.Helper()
	w := httptest.NewRecorder()
	owner.ServeMetrics("/metric/", w, httptest.NewRequest("GET", "/metric/operation/platform.records", nil))
	require.Equal(t, 200, w.Code)
	var catalog struct{ Count int64 }
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &catalog))
	return catalog.Count
}

func TestObservationSourceOwnerSeparateApplicationsSQLite(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(7)"))
	first, a := policyLifecycleRuntime(t, h.DB)
	second, b := policyLifecycleRuntime(t, h.DB)
	require.NotSame(t, first.Observability().Recorder, second.Observability().Recorder)
	require.NoError(t, policyInvoke(context.Background(), first, a))
	require.NoError(t, policyInvoke(context.Background(), first, a))
	require.Empty(t, second.Observability().Recorder.Values("platform.records"), "separate application has no capture or ghost counter")
	require.NoError(t, policyInvoke(context.Background(), second, b))
	require.EqualValues(t, 2, sourceCatalogCount(t, first.Observability().Recorder))
	require.EqualValues(t, 1, sourceCatalogCount(t, second.Observability().Recorder))
	require.EqualValues(t, 2, first.Observability().Recorder.Values("platform.records")["Success"])
	require.EqualValues(t, 1, second.Observability().Recorder.Values("platform.records")["Success"])
	require.Zero(t, first.Observability().Recorder.Values("platform.records")["Pending"])
	require.Zero(t, second.Observability().Recorder.Values("platform.records")["Pending"])
}
func TestObservationSourceOwnerTwoRootPendingBarrierSQLite(t *testing.T) {
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(7)"))
	app, a := policyLifecycleRuntime(t, h.DB)
	control := &policyLifecycleControl{started: make(chan struct{}, 2), release: make(chan struct{})}
	ctx := context.WithValue(context.Background(), policyLifecycleKey{}, control)
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { done <- policyInvoke(ctx, app, a) }()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-control.started:
		case <-time.After(3 * time.Second):
			close(control.release)
			t.Fatal("real root reads did not reach fetch barrier")
		}
	}
	require.EqualValues(t, 2, app.Observability().Recorder.Values("platform.records")["Pending"])
	if count := sourceCatalogCount(t, app.Observability().Recorder); count != 2 {
		t.Errorf("source total Count while actual root SQL blocked=%d, want2", count)
	}
	close(control.release)
	for i := 0; i < 2; i++ {
		require.NoError(t, <-done)
	}
	values := app.Observability().Recorder.Values("platform.records")
	require.Zero(t, values["Pending"])
	require.EqualValues(t, 2, values["Success"])
	require.Zero(t, values["Error"])
}

func TestObservationSourceOwnerFailureCancellationCleanupSQLite(t *testing.T) {
	for _, mode := range []string{"blocked-query-cancel", "acquisition-cancel", "row-panic", "query-failure"} {
		t.Run(mode, func(t *testing.T) {
			h := sqlite.New(t)
			require.NoError(t, h.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(7)"))
			db := h.DB
			started := make(chan struct{}, 1)
			if mode == "blocked-query-cancel" || mode == "query-failure" {
				db = h.FaultDB(t, func(ctx context.Context, c sqlfault.Call) error {
					if c.Phase != "query" {
						return nil
					}
					if mode == "query-failure" {
						return errors.New("driver private failure")
					}
					started <- struct{}{}
					<-ctx.Done()
					return ctx.Err()
				})
			}
			app, a := policyLifecycleRuntime(t, db)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var occupied *sql.Conn
			if mode == "acquisition-cancel" {
				db.SetMaxOpenConns(1)
				var err error
				occupied, err = db.Conn(context.Background())
				require.NoError(t, err)
				defer occupied.Close()
			}
			if mode == "row-panic" {
				ctx = context.WithValue(ctx, policyLifecycleKey{}, &policyLifecycleControl{panicRead: true})
			}
			ec := xexec.New()
			done := make(chan error, 1)
			go func() { done <- policyInvoke(xexec.WithContext(ctx, ec), app, a) }()
			if mode == "blocked-query-cancel" {
				select {
				case <-started:
				case <-time.After(3 * time.Second):
					t.Fatal("native query did not start")
				}
				require.EqualValues(t, 1, app.Observability().Recorder.Values("platform.records")["Pending"])
				if count := sourceCatalogCount(t, app.Observability().Recorder); count != 1 {
					t.Errorf("source total Count before cancellation=%d, want1", count)
				}
				cancel()
			}
			if mode == "acquisition-cancel" {
				deadline := time.After(3 * time.Second)
				for db.Stats().WaitCount == 0 {
					select {
					case <-deadline:
						t.Fatal("real DB acquisition did not block")
					default:
						time.Sleep(time.Millisecond)
					}
				}
				require.EqualValues(t, 1, app.Observability().Recorder.Values("platform.records")["Pending"])
				if count := sourceCatalogCount(t, app.Observability().Recorder); count != 1 {
					t.Errorf("source total Count before cancellation=%d, want1", count)
				}
				cancel()
			}
			select {
			case err := <-done:
				require.Error(t, err)
			case <-time.After(3 * time.Second):
				t.Fatal("read did not finish")
			}
			values := app.Observability().Recorder.Values("platform.records")
			require.Zero(t, values["Pending"])
			require.EqualValues(t, 1, values["Error"])
			require.Zero(t, values["Success"])
			require.EqualValues(t, 1, sourceCatalogCount(t, app.Observability().Recorder))
			require.Len(t, ec.Metrics, 1)
			require.Equal(t, "records#", ec.Metrics[0].View)
			require.NotEmpty(t, ec.Metrics[0].Error)
		})
	}
}
