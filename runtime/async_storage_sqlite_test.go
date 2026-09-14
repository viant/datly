package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	requestprovider "github.com/viant/bindly/provider/request"
	jobstorage "github.com/viant/datly/gateway/async"
	"github.com/viant/datly/internal/testharness"
	rhandler "github.com/viant/datly/runtime/handler"
	veltyhandler "github.com/viant/datly/runtime/handler/velty"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/route"
	sqldml "github.com/viant/datly/sql/dml"
	xasync "github.com/viant/xdatly/async"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type storageFixture struct {
	service        *jobs.Service
	dispatcher     *jobstorage.Dispatcher
	fs             afs.Service
	source, failed string
}

func (f *asyncReaderFixture) storage(t *testing.T, notify func(context.Context, *xasync.Job) error) *storageFixture {
	t.Helper()
	root := t.TempDir()
	fs := afs.New()
	source := filepath.Join(root, "jobs")
	failed := filepath.Join(root, "failed")
	publisher, err := jobstorage.NewPublisher(jobstorage.PublishConfig{FS: fs, Notification: xasync.Notification{Method: xasync.NotificationMethodStorage, Destination: source}})
	if err != nil {
		t.Fatal(err)
	}
	service, err := f.runtime.NewAsyncService(jobs.Config{Store: f.store, Publisher: publisher, Authorize: func(context.Context, jobs.Access) error { return nil }, Notify: notify})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher, err := jobstorage.NewDispatcher(fs, service)
	if err != nil {
		t.Fatal(err)
	}
	return &storageFixture{service: service, dispatcher: dispatcher, fs: fs, source: source, failed: failed}
}
func (s *storageFixture) watch(t *testing.T, max int) (context.CancelFunc, <-chan error) {
	t.Helper()
	watcher, err := jobstorage.NewWatcher(jobstorage.WatchConfig{JobURL: s.source, FailedJobURL: s.failed, MaxJobs: max, PollInterval: 5 * time.Millisecond}, s.dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- watcher.Run(ctx) }()
	return cancel, done
}
func waitAsyncStorage(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("async storage condition timed out")
		case <-tick.C:
		}
	}
}

func TestAsyncLocalStoragePublisherWatcherSQLite(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprintf("read_failure=%v", fail), func(t *testing.T) {
			f := newAsyncReaderFixture(t)
			ctx := context.Background()
			var notified atomic.Int32
			storage := f.storage(t, func(ctx context.Context, job *xasync.Job) error {
				row, err := f.store.Get(ctx, job.ID)
				if err != nil {
					return err
				}
				if row.EndTime == nil {
					return errors.New("notification before durable completion")
				}
				notified.Add(1)
				return nil
			})
			scheduled, err := storage.service.Schedule(ctx, f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
			if err != nil {
				t.Fatal(err)
			}
			payload, err := os.ReadFile(url.Path(scheduled.Job.EventURL))
			if err != nil {
				t.Fatal(err)
			}
			var event jobs.Event
			if err := json.Unmarshal(payload, &event); err != nil {
				t.Fatal(err)
			}
			if event.State == "" || event.ID != scheduled.Job.ID || len(event.SQL) == 0 || event.EventURL != scheduled.Job.EventURL {
				t.Fatalf("original event=%+v", event)
			}
			// Scheduling completed before the application table even existed.
			if !fail {
				if err := f.harness.ExecStatements(ctx, `CREATE TABLE async_users(id INTEGER,tenant INTEGER,name TEXT)`, `INSERT INTO async_users VALUES(7,1,'read through canonical reader')`); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(storage.source, "ignored.upload"), []byte("incomplete"), 0600); err != nil {
				t.Fatal(err)
			}
			cancel, done := storage.watch(t, 2)
			defer cancel()
			waitAsyncStorage(t, func() bool {
				_, err := os.Stat(url.Path(scheduled.Job.EventURL))
				return errors.Is(err, os.ErrNotExist)
			})
			cancel()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("watcher stop=%v", err)
			}
			row, err := f.store.Get(ctx, scheduled.Job.ID)
			if err != nil {
				t.Fatal(err)
			}
			want := xasync.StatusDone
			if fail {
				want = xasync.StatusError
			}
			if row.Status != want || notified.Load() != 1 {
				t.Fatalf("status=%s notifications=%d", row.Status, notified.Load())
			}
			failedPath := filepath.Join(storage.failed, time.Now().Format("20060102"), filepath.Base(url.Path(scheduled.Job.EventURL)))
			_, err = os.Stat(failedPath)
			if fail != (err == nil) {
				t.Fatalf("failure file=%v", err)
			}
			if _, err := os.Stat(filepath.Join(storage.source, "ignored.upload")); err != nil {
				t.Fatal("watcher consumed partial upload")
			}
		})
	}
}

func TestAsyncStorageDuplicateDispatchUsesDurableStateSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	ctx := context.Background()
	var calls atomic.Int32
	f.runtime.registered[f.target.Component.String()].Handler = rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
		if inv.Input.(*asyncReaderInput).ID != 7 {
			return nil, errors.New("event forged canonical input")
		}
		requestValue, requestFound, requestErr := inv.Binder.Lookup(ctx, xhandler.ValueKey(requestprovider.HTTPRequestKind))
		if requestErr != nil || !requestFound {
			return nil, fmt.Errorf("synthetic request unavailable: %v", requestErr)
		}
		synthetic := requestValue.(*http.Request)
		if synthetic.URL.Host != "localhost" || synthetic.RequestURI != "/jobs/users/7" {
			return nil, errors.New("synthetic request shape differs from original HandleJob")
		}
		invocation, found := jobs.CurrentInvocation(ctx)
		if !found || invocation.Type != xasync.InvocationTypeEvent || invocation.Job.ID == "" || invocation.Job.Status != xasync.StatusRunning || invocation.Job.StartTime == nil {
			return nil, errors.New("missing scoped async invocation")
		}
		inv.Response.AddMetric(&xresponse.Metric{View: "audit", Executions: xresponse.SQLExecutions{&xresponse.SQLExecution{SQL: "SELECT ?", Args: []any{7}}}})
		calls.Add(1)
		return nil, nil
	})
	storage := f.storage(t, nil)
	scheduled, err := storage.service.Schedule(ctx, f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := os.ReadFile(url.Path(scheduled.Job.EventURL))
	if err != nil {
		t.Fatal(err)
	}
	var event jobs.Event
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatal(err)
	}
	event.State = `{"ID":999}`
	payload, _ = json.Marshal(event)
	if err := os.WriteFile(url.Path(scheduled.Job.EventURL), payload, 0600); err != nil {
		t.Fatal(err)
	}
	object, err := storage.fs.Object(ctx, scheduled.Job.EventURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.harness.DB.Exec(`UPDATE DATLY_JOBS SET Deactivated=NULL WHERE ID=?`, scheduled.Job.ID); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := storage.dispatcher.DispatchStorageEvent(ctx, object); err != nil {
			t.Fatal(err)
		}
	}
	row, err := f.store.Get(ctx, scheduled.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(row.Metrics, "audit") || !strings.Contains(row.SQLQuery, "SELECT ?") {
		t.Fatalf("original metrics/query columns lost: %+v", row)
	}
	if calls.Load() != 1 {
		t.Fatalf("duplicate delivery executed %d times", calls.Load())
	}
	event.URI = "/forged"
	if _, err := storage.service.HandleJob(ctx, &event); err == nil {
		t.Fatal("forged routing envelope accepted")
	}
}

func TestAsyncWatcherConcurrencyAndCancellationSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	var active, max atomic.Int32
	f.runtime.registered[f.target.Component.String()].Handler = rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
		count := active.Add(1)
		defer active.Add(-1)
		for old := max.Load(); count > old && !max.CompareAndSwap(old, count); old = max.Load() {
		}
		entered <- struct{}{}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return nil, nil
		}
	})
	storage := f.storage(t, nil)
	for i := 0; i < 6; i++ {
		if _, err := storage.service.Schedule(context.Background(), f.submission(&asyncReaderInput{ID: 7, Tenant: 1})); err != nil {
			t.Fatal(err)
		}
	}
	cancel, done := storage.watch(t, 2)
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("watcher did not start jobs")
		}
	}
	select {
	case <-entered:
		t.Fatal("MaxJobs exceeded")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server cancellation did not join workers")
	}
	if active.Load() != 0 || max.Load() != 2 {
		t.Fatalf("active=%d max=%d", active.Load(), max.Load())
	}
	pending, err := f.store.Pending(context.Background(), 10)
	if err != nil || len(pending) != 4 {
		t.Fatalf("unstarted jobs=%d err=%v", len(pending), err)
	}
	close(release)
}

func TestAsyncWatcherConcurrencyIsScopedSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	entered := make(chan struct{}, 2)
	f.runtime.registered[f.target.Component.String()].Handler = rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	first, second := f.storage(t, nil), f.storage(t, nil)
	for _, storage := range []*storageFixture{first, second} {
		if _, err := storage.service.Schedule(context.Background(), f.submission(&asyncReaderInput{ID: 7, Tenant: 1})); err != nil {
			t.Fatal(err)
		}
	}
	c1, d1 := first.watch(t, 1)
	defer c1()
	c2, d2 := second.watch(t, 1)
	defer c2()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("watchers shared a global limiter")
		}
	}
	c1()
	c2()
	<-d1
	<-d2
}

func TestAsyncCustomMutationCompletionStorageSQLite(t *testing.T) {
	for _, tc := range []struct {
		name                         string
		external, panicHandler, fail bool
		status                       xasync.Status
	}{{"commit", false, false, false, xasync.StatusDone}, {"rollback", false, false, true, xasync.StatusError}, {"panic rollback", false, true, false, xasync.StatusError}, {"caller pending", true, false, false, xasync.StatusRunning}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newAsyncReaderFixture(t)
			app := testharness.NewSQLiteHarness(t)
			ctx := context.Background()
			if err := app.ExecStatements(ctx, `CREATE TABLE audit(id INTEGER)`); err != nil {
				t.Fatal(err)
			}
			var tx *sql.Tx
			var err error
			if tc.external {
				tx, err = app.DB.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
			}
			registered := f.runtime.registered[f.target.Component.String()]
			registered.DataSource = sqldml.Source{DB: app.DB, Tx: tx}
			registered.Handler = rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
				value, found, err := inv.Binder.Lookup(ctx, xhandler.DataKey)
				if err != nil || !found {
					return nil, fmt.Errorf("canonical Data unavailable: %v", err)
				}
				starter, found, startErr := inv.Binder.Lookup(ctx, xhandler.TransactionStarterKey)
				if startErr != nil || !found {
					return nil, fmt.Errorf("canonical starter unavailable: %v", startErr)
				}
				if err := starter.(xhandler.TransactionStarter).Start(ctx); err != nil {
					return nil, err
				}
				if err := value.(xhandler.Data).Execute("INSERT INTO audit(id) VALUES (?)", inv.Input.(*asyncReaderInput).ID); err != nil {
					return nil, err
				}
				if err := value.(xhandler.Data).Flush(ctx, ""); err != nil {
					return nil, err
				}
				if tc.panicHandler {
					panic("application panic")
				}
				if tc.fail {
					return nil, errors.New("application failed")
				}
				return nil, nil
			})
			var notified atomic.Int32
			storage := f.storage(t, func(ctx context.Context, j *xasync.Job) error {
				notified.Add(1)
				if j.Status == xasync.StatusDone {
					var count int
					if err := app.DB.QueryRowContext(ctx, `SELECT count(*) FROM audit`).Scan(&count); err != nil {
						return err
					}
					if count != 1 {
						return errors.New("notification preceded actual commit")
					}
				}
				return nil
			})
			scheduled, err := storage.service.Schedule(ctx, f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
			if err != nil {
				t.Fatal(err)
			}
			object, err := storage.fs.Object(ctx, scheduled.Job.EventURL)
			if err != nil {
				t.Fatal(err)
			}
			err = storage.dispatcher.DispatchStorageEvent(ctx, object)
			if tc.external && !errors.Is(err, jobs.ErrCompletionPending) {
				t.Fatalf("pending completion=%v", err)
			}
			if (tc.external || tc.fail || tc.panicHandler) != (err != nil) {
				t.Fatalf("dispatch error=%v", err)
			}
			row, err := f.store.Get(ctx, scheduled.Job.ID)
			if err != nil {
				t.Fatal(err)
			}
			if row.Status != tc.status {
				t.Fatalf("status=%s", row.Status)
			}
			if tc.external {
				if notified.Load() != 0 || row.EndTime != nil {
					t.Fatal("caller-pending completion falsely confirmed")
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
			}
			var count int
			if err := app.DB.QueryRow(`SELECT count(*) FROM audit`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			want := 0
			if tc.status == xasync.StatusDone {
				want = 1
			}
			if count != want {
				t.Fatalf("application rows=%d want=%d", count, want)
			}
		})
	}
}

func TestAsyncLocalPublicationFailureKeepsDurableJobSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	ctx := context.Background()
	root := t.TempDir()
	blocked := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(blocked, []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	publisher, err := jobstorage.NewPublisher(jobstorage.PublishConfig{Notification: xasync.Notification{Method: xasync.NotificationMethodStorage, Destination: blocked}})
	if err != nil {
		t.Fatal(err)
	}
	service, err := f.runtime.NewAsyncService(jobs.Config{Store: f.store, Publisher: publisher, Authorize: func(context.Context, jobs.Access) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Schedule(ctx, f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
	var publication *jobs.PublicationError
	if !errors.As(err, &publication) {
		t.Fatalf("publication error=%v", err)
	}
	row, err := f.store.Get(ctx, publication.ID)
	if err != nil || row.Status != xasync.StatusPending || !strings.HasSuffix(row.EventURL, ".job") {
		t.Fatalf("durable pending=%+v err=%v", row, err)
	}
	if err := os.Remove(blocked); err != nil {
		t.Fatal(err)
	}
	if err := service.Republish(ctx, row.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(url.Path(row.EventURL)); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncVeltyStorageDispatchSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	ctx := context.Background()
	app := testharness.NewSQLiteHarness(t)
	if err := app.ExecStatements(ctx, `CREATE TABLE audit(id INTEGER)`); err != nil {
		t.Fatal(err)
	}
	handler, err := veltyhandler.New[asyncReaderInput, asyncReaderOutput](veltyhandler.Config{Template: `$dml.Execute("INSERT INTO audit(id) VALUES (?)", $ID)`})
	if err != nil {
		t.Fatal(err)
	}
	registered := f.runtime.registered[f.target.Component.String()]
	registered.Handler = handler
	registered.DataSource = sqldml.Source{DB: app.DB}
	storage := f.storage(t, nil)
	scheduled, err := storage.service.Schedule(ctx, f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if scheduled.Plan != nil {
		t.Fatal("Velty scheduling advertised a SQL dry-run")
	}
	object, err := storage.fs.Object(ctx, scheduled.Job.EventURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.dispatcher.DispatchStorageEvent(ctx, object); err != nil {
		t.Fatal(err)
	}
	var id int
	if err := app.DB.QueryRow(`SELECT id FROM audit`).Scan(&id); err != nil || id != 7 {
		t.Fatalf("canonical Velty DML=%d err=%v", id, err)
	}
	row, err := f.store.Get(ctx, scheduled.Job.ID)
	if err != nil || row.Status != xasync.StatusDone {
		t.Fatalf("status=%+v err=%v", row, err)
	}
}

type asyncProvenanceHandler struct{ rhandler.Handler }

func (asyncProvenanceHandler) RequiresReadMetadata() bool { return true }
func TestAsyncProvenanceSubmissionDefersHandlerSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	called := false
	f.runtime.registered[f.target.Component.String()].Handler = asyncProvenanceHandler{rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { called = true; return nil, nil })}
	storage := f.storage(t, nil)
	if _, err := storage.service.Schedule(context.Background(), f.submission(&asyncReaderInput{ID: 7, Tenant: 1})); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("provenance mutation executed")
	}
	pending, err := f.store.Pending(context.Background(), 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("deferred mutation was not persisted: %v %v", pending, err)
	}
}

func TestAsyncUnknownCommitRetainsEventSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	app := testharness.NewSQLiteHarness(t)
	app.DB.SetMaxOpenConns(1)
	ctx := context.Background()
	if err := app.ExecStatements(ctx, `PRAGMA foreign_keys=ON`, `CREATE TABLE parent(id INTEGER PRIMARY KEY)`, `CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)`); err != nil {
		t.Fatal(err)
	}
	registered := f.runtime.registered[f.target.Component.String()]
	registered.DataSource = sqldml.Source{DB: app.DB}
	registered.Handler = rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
		value, _, err := inv.Binder.Lookup(ctx, xhandler.DataKey)
		if err != nil {
			return nil, err
		}
		return nil, value.(xhandler.Data).Execute("INSERT INTO child VALUES(99)")
	})
	var notified atomic.Int32
	storage := f.storage(t, func(context.Context, *xasync.Job) error { notified.Add(1); return nil })
	scheduled, err := storage.service.Schedule(ctx, f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
	if err != nil {
		t.Fatal(err)
	}
	object, err := storage.fs.Object(ctx, scheduled.Job.EventURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.dispatcher.DispatchStorageEvent(ctx, object); !errors.Is(err, jobs.ErrCompletionPending) {
		t.Fatalf("unknown commit=%v", err)
	}
	row, err := f.store.Get(ctx, scheduled.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != xasync.StatusRunning || row.EndTime != nil || notified.Load() != 0 {
		t.Fatalf("unknown commit falsely completed: %+v notifications=%d", row, notified.Load())
	}
	if _, err := os.Stat(url.Path(scheduled.Job.EventURL)); err != nil {
		t.Fatal(err)
	}
}

func TestAsyncStorageInvokesAuthorizedInternalComponentSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	ctx := context.Background()
	hidden, err := route.NewBundle(nil)
	if err != nil {
		t.Fatal(err)
	}
	f.runtime.publicBundle = hidden
	calls := 0
	f.runtime.registered[f.target.Component.String()].Handler = rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) { calls++; return nil, nil })
	if f.runtime.ExposesComponent(f.target.Component) {
		t.Fatal("fixture exposed internal component")
	}
	storage := f.storage(t, nil)
	scheduled, err := storage.service.Schedule(ctx, f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
	if err != nil {
		t.Fatal(err)
	}
	object, err := storage.fs.Object(ctx, scheduled.Job.EventURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.dispatcher.DispatchStorageEvent(ctx, object); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || f.runtime.ExposesComponent(f.target.Component) {
		t.Fatal("storage invocation changed HTTP catalog exposure")
	}
}

func TestAsyncWatcherRetainsFileWhenFailureArchiveFailsSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	storage := f.storage(t, nil)
	ctx := context.Background()
	scheduled, err := storage.service.Schedule(ctx, f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(storage.failed, []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	var reported atomic.Int32
	watcher, err := jobstorage.NewWatcher(jobstorage.WatchConfig{JobURL: storage.source, FailedJobURL: storage.failed, MaxJobs: 1, PollInterval: 5 * time.Millisecond, Error: func(string, error) { reported.Add(1) }}, storage.dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	watchCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- watcher.Run(watchCtx) }()
	waitAsyncStorage(t, func() bool { return reported.Load() >= 2 })
	cancel()
	<-done
	if _, err := os.Stat(url.Path(scheduled.Job.EventURL)); err != nil {
		t.Fatal("source event lost after failed archive")
	}
	row, err := f.store.Get(ctx, scheduled.Job.ID)
	if err != nil || row.Status != xasync.StatusError {
		t.Fatalf("status=%+v error=%v", row, err)
	}
}

func TestAsyncWatcherArchivesMalformedEventSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	storage := f.storage(t, nil)
	ctx := context.Background()
	source := filepath.Join(storage.source, "broken.job")
	if err := storage.fs.Upload(ctx, source, 0600, strings.NewReader("{broken")); err != nil {
		t.Fatal(err)
	}
	cancel, done := storage.watch(t, 1)
	defer cancel()
	destination := filepath.Join(storage.failed, time.Now().Format("20060102"), "broken.job")
	waitAsyncStorage(t, func() bool { _, err := os.Stat(destination); return err == nil })
	cancel()
	<-done
	if _, err := os.Stat(source); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("malformed source not archived: %v", err)
	}
	var count int
	if err := f.harness.DB.QueryRow(`SELECT count(*) FROM DATLY_JOBS`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("malformed event altered durable jobs: count=%d err=%v", count, err)
	}
}

func TestAsyncWatcherRetainsTerminalPersistenceFailureSQLite(t *testing.T) {
	f := newAsyncReaderFixture(t)
	ctx := context.Background()
	if err := f.harness.ExecStatements(ctx, `CREATE TABLE executed(id INTEGER)`, `CREATE TRIGGER fail_job_completion BEFORE UPDATE OF Status ON DATLY_JOBS WHEN NEW.Status IN ('DONE','ERROR') BEGIN SELECT RAISE(ABORT,'terminal write unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	var calls, notified atomic.Int32
	registered := f.runtime.registered[f.target.Component.String()]
	registered.DataSource = sqldml.Source{DB: f.harness.DB}
	registered.Handler = rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
		calls.Add(1)
		value, _, err := inv.Binder.Lookup(ctx, xhandler.DataKey)
		if err != nil {
			return nil, err
		}
		return nil, value.(xhandler.Data).Execute(`INSERT INTO executed VALUES(1)`)
	})
	storage := f.storage(t, func(context.Context, *xasync.Job) error { notified.Add(1); return nil })
	scheduled, err := storage.service.Schedule(ctx, f.submission(&asyncReaderInput{ID: 7, Tenant: 1}))
	if err != nil {
		t.Fatal(err)
	}
	reported := make(chan error, 1)
	watcher, err := jobstorage.NewWatcher(jobstorage.WatchConfig{JobURL: storage.source, FailedJobURL: storage.failed, MaxJobs: 1, PollInterval: 5 * time.Millisecond, Error: func(_ string, e error) {
		select {
		case reported <- e:
		default:
		}
	}}, storage.dispatcher)
	if err != nil {
		t.Fatal(err)
	}
	watchCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- watcher.Run(watchCtx) }()
	defer func() { cancel(); <-done }()
	select {
	case err = <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("missing reconciliation error")
	}
	if !errors.Is(err, jobs.ErrCompletionPending) {
		t.Fatalf("failure not classified for reconciliation: %v", err)
	}
	row, err := f.store.Get(ctx, scheduled.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != xasync.StatusRunning || row.EndTime != nil || notified.Load() != 0 {
		t.Fatalf("row=%+v notifications=%d", row, notified.Load())
	}
	if _, err := os.Stat(url.Path(scheduled.Job.EventURL)); err != nil {
		t.Fatalf("source event lost: %v", err)
	}
	var count int
	if err := f.harness.DB.QueryRow(`SELECT COUNT(*) FROM executed`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("execution count=%d err=%v", count, err)
	}
	object, err := storage.fs.Object(ctx, scheduled.Job.EventURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.dispatcher.DispatchStorageEvent(ctx, object); !errors.Is(err, jobs.ErrInProgress) {
		t.Fatalf("repeat delivery=%v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("duplicate execution=%d", calls.Load())
	}
}
