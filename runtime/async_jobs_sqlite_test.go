package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	rsql "github.com/viant/datly/sql"

	xasync "github.com/viant/xdatly/async"
)

type asyncReaderInput struct {
	ID            int
	Tenant        int
	Values        []int
	RejectAmbient bool
	Panic         bool
}
type asyncAmbientKey struct{}

func (i *asyncReaderInput) Init(ctx context.Context) error {
	if i.Panic || i.ID == -7 {
		panic("replay input panic")
	}
	if ctx.Value(asyncAmbientKey{}) != nil {
		return errors.New("caller context leaked")
	}
	return nil
}

type asyncReaderRow struct {
	ID   int
	Name string
}
type asyncReaderOutput struct{ Data []*asyncReaderRow }

type asyncReaderFixture struct {
	runtime *Runtime
	harness *testharness.Harness
	store   *jobs.SQLStore
	target  dexec.ComponentTarget
}

func newAsyncReaderFixture(t *testing.T, settings ...*spec.Settings) *asyncReaderFixture {
	t.Helper()
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(), sqlite.DatlyJobsSchema); err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/jobs", Name: "Users"}, Name: "Users",
		Routes:   []*spec.Route{{Method: "GET", Path: "/jobs/users/{id}"}},
		RootView: &spec.View{Name: "Users", Selector: &spec.Selector{DefaultLimit: 5}, Source: &spec.ViewSource{SQL: "SELECT id, name FROM async_users WHERE id = :ID AND tenant = :Tenant"}},
		Parameters: []*spec.Parameter{
			{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}},
			{Name: "Tenant", Source: spec.BindSource{Kind: "query", Name: "tenant"}},
			{Name: "Values", Source: spec.BindSource{Kind: "query", Name: "values"}},
			{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
	}
	if len(settings) > 0 {
		component.Settings = settings[0]
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(asyncReaderInput{}), OutputType: reflect.TypeOf(asyncReaderOutput{}), DirectViewField: "Data"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &rsql.SQLComponent{DB: h.DB}})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := route.NewBundle([]*spec.Component{component})
	if err != nil {
		t.Fatal(err)
	}
	runtime := newTestService(t, bundle, map[string]*registry.RegisteredComponent{component.Key.String(): {Component: component, Input: artifact.Input, OutputType: reflect.TypeOf(asyncReaderOutput{}), Reader: reader}})
	store, err := (bootstrap.JobStoreConfig{SQL: &rsql.SQLComponent{DB: h.DB}}).NewStore(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return &asyncReaderFixture{runtime: runtime, harness: h, store: store, target: dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/jobs/users/{id}"}}}
}
func (f *asyncReaderFixture) submission(input *asyncReaderInput) jobs.Submission {
	return jobs.Submission{Job: xasync.Job{Request: xasync.Request{Method: "GET", URI: "/jobs/users/7"}, MatchKey: "users-7", MainView: "Users"}, Input: input}
}

func TestAsyncOriginalSchemaDryRunReplaySQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	ctx := context.Background()
	notifications := 0
	policyCalls := []jobs.Action{}
	service, err := f.runtime.NewAsyncService(jobs.Config{Store: f.store, Authorize: func(_ context.Context, access jobs.Access) error {
		policyCalls = append(policyCalls, access.Action)
		if access.Action == jobs.Replay {
			input := access.Input.(*asyncReaderInput)
			if input.Values[0] != 3 {
				t.Errorf("caller mutation leaked into replay: %+v", input)
			}
			input.RejectAmbient = true
			input.Tenant = 2 // refreshed authorization scope comes from trusted policy
		}
		return nil
	}, Notify: func(ctx context.Context, job *xasync.Job) error {
		stored, err := f.store.Get(ctx, job.ID)
		if err != nil {
			return err
		}
		if stored.Status != xasync.StatusDone || job.EndTime == nil {
			t.Errorf("notification before durable completion: %+v", job)
		}
		notifications++
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	input := &asyncReaderInput{ID: 7, Tenant: 1, Values: []int{3}}
	// The application table is absent. Opening an SQLX row reader here would fail.
	scheduled, err := service.Schedule(ctx, f.submission(input))
	if err != nil {
		t.Fatalf("dry-run read application rows: %v", err)
	}
	if !strings.Contains(scheduled.Plan.SQL, "async_users") || len(scheduled.Plan.Args) < 2 {
		t.Fatalf("plan=%+v", scheduled.Plan)
	}
	if notifications != 0 || scheduled.Job.Status != xasync.StatusPending {
		t.Fatalf("premature completion: %+v", scheduled.Job)
	}
	input.ID, input.Values[0] = 99, 99
	stored, err := f.store.Get(ctx, scheduled.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stored.State, `"ID":7`) || !strings.Contains(stored.SQLQuery, `"Query"`) {
		t.Fatalf("original encoding: %+v", stored)
	}
	if err := f.harness.ExecStatements(ctx, `CREATE TABLE async_users(id INTEGER, tenant INTEGER, name TEXT)`, `INSERT INTO async_users VALUES (7,1,'old-scope'),(7,2,'current-scope')`); err != nil {
		t.Fatal(err)
	}
	result, err := service.Run(context.WithValue(ctx, asyncAmbientKey{}, "caller transaction or request"), scheduled.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	output := result.(*asyncReaderOutput)
	if len(output.Data) != 1 || output.Data[0].Name != "current-scope" {
		t.Fatalf("replay=%+v", output)
	}
	public, err := service.Status(ctx, scheduled.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if public.Status != xasync.StatusDone || public.StartTime == nil || public.EndTime == nil || public.ExpiryTime == nil || notifications != 1 {
		t.Fatalf("status=%+v, notifications=%d", public, notifications)
	}
	if _, err := service.Run(ctx, scheduled.Job.ID); !errors.Is(err, jobs.ErrTransition) {
		t.Fatalf("duplicate execution: %v", err)
	}
	if len(policyCalls) != 3 {
		t.Fatalf("authorization=%v", policyCalls)
	}
}

func TestAsyncOriginalStateAndFailureSQLite(t *testing.T) {
	for _, tc := range []struct {
		name, state string
		denied      bool
		wantStatus  xasync.Status
	}{
		{name: "application read error", state: `{"ID":7,"Tenant":1,"Values":[3],"Query":"unused original cache entry"}`, wantStatus: xasync.StatusError},
		{name: "authorization denied", state: `{"ID":7,"Tenant":1}`, denied: true, wantStatus: xasync.StatusPending},
		{name: "malformed snapshot", state: `{"ID":`, wantStatus: xasync.StatusPending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncReaderFixture(t)
			record := &jobs.Record{Job: xasync.Job{ID: "original", Status: xasync.StatusPending, Request: xasync.Request{Method: "GET", URI: "/jobs/users/7"}, CreationTime: time.Now().UTC()}, State: tc.state}
			if err := f.store.Create(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			notified := false
			service, err := f.runtime.NewAsyncService(jobs.Config{Store: f.store, Authorize: func(context.Context, jobs.Access) error {
				if tc.denied {
					return errors.New("denied")
				}
				return nil
			}, Notify: func(_ context.Context, j *xasync.Job) error {
				notified = true
				if j.Status != xasync.StatusError {
					t.Errorf("status=%s", j.Status)
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.Run(context.Background(), record.ID); err == nil {
				t.Fatal("expected replay failure")
			}
			stored, err := f.store.Get(context.Background(), record.ID)
			if err != nil {
				t.Fatal(err)
			}
			if stored.Status != tc.wantStatus {
				t.Fatalf("status=%s", stored.Status)
			}
			if notified != (tc.wantStatus == xasync.StatusError) {
				t.Fatalf("notified=%v", notified)
			}
			if tc.wantStatus == xasync.StatusError && (stored.Error == nil || stored.EndTime == nil || stored.ExpiryTime.Sub(*stored.EndTime) != 10*time.Second) {
				t.Fatalf("error timing=%+v", stored)
			}
		})
	}
}

func TestAsyncExpiryAndAuthorizationRequiredSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	if _, err := f.runtime.NewAsyncService(jobs.Config{Store: f.store}); err == nil {
		t.Fatal("missing authorization accepted")
	}
	now := time.Now().UTC()
	expired := now.Add(-time.Minute)
	for _, status := range []xasync.Status{xasync.StatusPending, xasync.StatusRunning, xasync.StatusDone, xasync.StatusError} {
		record := &jobs.Record{Job: xasync.Job{ID: string(status), Status: xasync.StatusPending, Request: xasync.Request{Method: "GET", URI: "/jobs/users/7"}, CreationTime: expired, ExpiryTime: &expired}, State: `{}`}
		if err := f.store.Create(context.Background(), record); err != nil {
			t.Fatal(err)
		}
		if _, err := f.harness.DB.Exec(`UPDATE DATLY_JOBS SET Status = ? WHERE ID = ?`, status, record.ID); err != nil {
			t.Fatal(err)
		}
	}
	count, err := f.store.Expire(context.Background(), now)
	if err != nil || count != 3 {
		t.Fatalf("expiry count=%d: %v", count, err)
	}
	pending, err := f.store.Pending(context.Background(), 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending=%v: %v", pending, err)
	}
	running, err := f.store.Get(context.Background(), string(xasync.StatusRunning))
	if err != nil || running.Deactivated {
		t.Fatalf("running=%+v: %v", running, err)
	}
}

func TestAsyncClaimAndTerminalNotificationSQLite(t *testing.T) {
	for _, tc := range []struct {
		name        string
		panicInput  bool
		notifyError bool
	}{
		{name: "concurrent delivery"}, {name: "panic terminal failure", panicInput: true}, {name: "notification failure", notifyError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncReaderFixture(t)
			ctx := context.Background()
			if err := f.harness.ExecStatements(ctx, `CREATE TABLE async_users(id INTEGER, tenant INTEGER, name TEXT)`, `INSERT INTO async_users VALUES(7,1,'one')`); err != nil {
				t.Fatal(err)
			}
			barrier := make(chan struct{})
			var arrivals atomic.Int32
			var notifications atomic.Int32
			service, err := f.runtime.NewAsyncService(jobs.Config{Store: f.store, Authorize: func(_ context.Context, a jobs.Access) error {
				if a.Action == jobs.Replay {
					if tc.panicInput {
						a.Input.(*asyncReaderInput).ID = -7
					}
					if arrivals.Add(1) == 2 {
						close(barrier)
					}
					<-barrier
				}
				return nil
			}, Notify: func(_ context.Context, j *xasync.Job) error {
				notifications.Add(1)
				if tc.notifyError {
					return errors.New("notification unavailable")
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			scheduled, err := service.Schedule(ctx, f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			outcomes := make(chan error, 2)
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); _, err := service.Run(ctx, scheduled.Job.ID); outcomes <- err }()
			}
			wg.Wait()
			close(outcomes)
			rejected, executed := 0, 0
			for err := range outcomes {
				if errors.Is(err, jobs.ErrTransition) {
					rejected++
				} else {
					executed++
					if (tc.panicInput || tc.notifyError) != (err != nil) {
						t.Errorf("execution error=%v", err)
					}
				}
			}
			if rejected != 1 || executed != 1 || notifications.Load() != 1 {
				t.Fatalf("claims=%d/%d notifications=%d", rejected, executed, notifications.Load())
			}
			stored, err := f.store.Get(ctx, scheduled.Job.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := xasync.StatusDone
			if tc.panicInput {
				want = xasync.StatusError
			}
			if stored.Status != want {
				t.Fatalf("terminal=%s", stored.Status)
			}
		})
	}
}

func TestReaderDryRunKeepsPaginationSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	value, err := f.runtime.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: f.target, Input: &asyncReaderInput{ID: 7, Tenant: 1}, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	plan := value.(*dexec.ReadPlan)
	if !strings.Contains(strings.ToUpper(plan.SQL), "LIMIT 5") {
		t.Fatalf("reader limit lost: %s", plan.SQL)
	}
	prepared, err := f.runtime.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: f.target, Input: &asyncReaderInput{ID: 7, Tenant: 1}, PrepareQuery: true})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToUpper(prepared.(*dexec.PreparedQuery).SQL), "LIMIT") {
		t.Fatalf("query composition control unexpectedly windowed")
	}
}

func TestAsyncReplayRefreshesNativeCacheSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t, &spec.Settings{Cache: &spec.CacheSettings{Enabled: true, Name: "async-users", Location: t.TempDir(), TTL: "1m"}})
	ctx := context.Background()
	if err := f.harness.ExecStatements(ctx, `CREATE TABLE async_users(id INTEGER, tenant INTEGER, name TEXT)`, `INSERT INTO async_users VALUES(7,1,'before')`); err != nil {
		t.Fatal(err)
	}
	request := dexec.ComponentRequest{Target: f.target, Input: &asyncReaderInput{ID: 7, Tenant: 1}}
	initial, err := f.runtime.InvokeComponent(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if initial.(*asyncReaderOutput).Data[0].Name != "before" {
		t.Fatal(initial)
	}
	if err := f.harness.ExecStatements(ctx, `UPDATE async_users SET name='after'`); err != nil {
		t.Fatal(err)
	}
	cached, err := f.runtime.InvokeComponent(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if cached.(*asyncReaderOutput).Data[0].Name != "before" {
		t.Fatalf("control did not reuse cache: %+v", cached)
	}
	service, err := f.runtime.NewAsyncService(jobs.Config{Store: f.store, Authorize: func(context.Context, jobs.Access) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	scheduled, err := service.Schedule(ctx, f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
	if err != nil {
		t.Fatal(err)
	}
	refreshed, err := service.Run(ctx, scheduled.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.(*asyncReaderOutput).Data[0].Name != "after" {
		t.Fatalf("event replay used stale cache: %+v", refreshed)
	}
	latest, err := f.runtime.InvokeComponent(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if latest.(*asyncReaderOutput).Data[0].Name != "after" {
		t.Fatalf("event replay did not refresh native cache: %+v", latest)
	}
}

func TestAsyncCustomHandlerHasNoMutationDryRunSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	called := false
	f.runtime.registered[f.target.Component.String()].Handler = rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { called = true; return nil, nil })
	service, err := f.runtime.NewAsyncService(jobs.Config{Store: f.store, Authorize: func(context.Context, jobs.Access) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	scheduled, err := service.Schedule(context.Background(), f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if scheduled.Plan != nil || called {
		t.Fatal("custom handler ran while scheduling")
	}
	if _, err := f.runtime.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: f.target, Input: &asyncReaderInput{}, DryRun: true}); err == nil {
		t.Fatal("custom handler dry-run accepted")
	}
	if _, err := service.Run(context.Background(), scheduled.Job.ID); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("custom handler was not invoked")
	}
}
