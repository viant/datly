package standalone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/afs"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/datly/application"
	authfixture "github.com/viant/datly/internal/testharness/auth"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/asyncrecords"
	"github.com/viant/scy/auth/jwt"
	xasync "github.com/viant/xdatly/async"
	xresponse "github.com/viant/xdatly/response"
)

type standaloneAsyncFixture struct {
	*fixture.Fixture
	jwt          *authfixture.JWT
	cfg          *config.Config
	host         AsyncOptions
	server       *Server
	jobDB        *sqlite.Harness
	denied       atomic.Bool
	replayDenied atomic.Bool
	calls        atomic.Int32
	handlerGate  func(context.Context) error
	authCalls    [3]atomic.Int32
	grants       sync.Map
}

func newStandaloneAsync(t *testing.T) *standaloneAsyncFixture {
	t.Helper()
	f := &standaloneAsyncFixture{Fixture: fixture.New(t), jwt: authfixture.NewJWT(t)}
	jobDSN := filepath.Join(f.Root, "jobs.db")
	f.jobDB = sqlite.New(t, sqlite.WithDSN(jobDSN))
	require.NoError(t, f.jobDB.ExecStatements(context.Background(), strings.Replace(sqlite.DatlyJobsSchema, "DATLY_JOBS", "APP_JOBS", 1)))
	f.WriteConfig(t, func(c map[string]any) {
		c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/asyncrecords"}}
		c["Connectors"] = []any{map[string]any{"Name": "main", "Driver": "sqlite3", "DSN": f.DSN}, map[string]any{"Name": "jobs", "Driver": "sqlite3", "DSN": jobDSN}}
		c["JWTValidator"] = f.jwt.Config
		c["Jobs"] = map[string]any{"Connector": "jobs", "Table": "APP_JOBS", "DisableTableCreation": true, "TTLSeconds": 120, "ErrorTTLSeconds": 30, "Notification": map[string]any{"Method": "Storage", "Destination": "events"}}
		c["APIKeys"] = []any{map[string]any{"URI": "/async-", "Header": "X-Key", "Value": "allowed"}}
		c["Async"] = []any{
			map[string]any{"Route": map[string]any{"Method": "POST", "Path": "/async-records"}, "MatchKey": "Key", "SyncFlag": "Sync"},
			map[string]any{"Route": map[string]any{"Method": "GET", "Path": "/async-status/{jobid}"}, "Inspect": map[string]any{"JobID": "JobID", "Target": map[string]any{"Method": "POST", "Path": "/async-records"}}},
			map[string]any{"Route": map[string]any{"Method": "GET", "Path": "/async-read/{id}"}, "MatchKey": "Key", "SyncFlag": "Sync"},
		}
	})
	var err error
	f.cfg, err = (config.Loader{}).Load(context.Background(), f.Config)
	require.NoError(t, err)
	require.True(t, strings.HasSuffix(f.cfg.Jobs.Notification.Destination, "/events"))
	f.host = AsyncOptions{FS: afs.New(), Authorize: func(ctx context.Context, a jobs.Access) error {
		// The worker has no HTTP principal. A trusted grant issued at submission,
		// checked against current ACL, permits inspection of this exact identity.
		// Replay still requires the canonical verifier and a separate policy check.
		if a.Action == jobs.Inspect && a.Input == nil {
			f.authCalls[2].Add(1)
			_, granted := f.grants.Load(a.Job.ID)
			if !granted || f.denied.Load() {
				return fmt.Errorf("worker grant denied")
			}
			return ctx.Err()
		}
		var claim *jwt.Claims
		switch input := a.Input.(type) {
		case *asyncrecords.Input:
			claim = input.JWT
			if input.Initialized {
				return fmt.Errorf("authorization ran after Init")
			}
		case *asyncrecords.StatusInput:
			claim = input.JWT
		case *asyncrecords.ReadInput:
			claim = input.JWT
		default:
			return fmt.Errorf("unrecognized authorization input")
		}
		switch a.Action {
		case jobs.Submit:
			f.authCalls[0].Add(1)
		case jobs.Replay:
			f.authCalls[1].Add(1)
		case jobs.Inspect:
			f.authCalls[2].Add(1)
		default:
			return fmt.Errorf("unrecognized action")
		}
		if claim == nil || claim.Subject != "approved" || f.denied.Load() || a.Action == jobs.Replay && f.replayDenied.Load() {
			return &xresponse.Error{Code: 403, Payload: xresponse.Status{Status: "error", Message: "denied"}}
		}
		if a.Action == jobs.Submit {
			f.grants.Store(a.Job.ID, true)
		}
		return ctx.Err()
	}}
	return f
}
func (f *standaloneAsyncFixture) start(t *testing.T) {
	t.Helper()
	registry, err := asyncrecords.Exports(func(ctx context.Context) error {
		f.calls.Add(1)
		if f.handlerGate != nil {
			return f.handlerGate(ctx)
		}
		return nil
	})
	require.NoError(t, err)
	f.server, err = New(context.Background(), Options{Config: f.cfg, Registry: registry, Async: &f.host})
	require.NoError(t, err)
	s := f.server
	t.Cleanup(func() { require.NoError(t, s.Shutdown(context.Background())) })
	require.NoError(t, s.Reload(context.Background(), 1))
}
func (f *standaloneAsyncFixture) request(method, path, body, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("X-Key", "allowed")
	res := httptest.NewRecorder()
	f.server.ServeHTTP(res, req)
	return res
}
func (f *standaloneAsyncFixture) schedule(t *testing.T, key, token string) *xasync.Job {
	t.Helper()
	res := f.request("POST", "/async-records?key="+key, `{"data":{"id":2,"name":"created"}}`, token)
	require.Equal(t, 200, res.Code, res.Body.String())
	var out asyncrecords.Output
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &out))
	require.NotNil(t, out.Job)
	return out.Job
}
func (f *standaloneAsyncFixture) status(t *testing.T, id string) xasync.Status {
	t.Helper()
	var status string
	require.NoError(t, f.jobDB.DB.QueryRow("SELECT Status FROM APP_JOBS WHERE ID=?", id).Scan(&status))
	return xasync.Status(status)
}
func (f *standaloneAsyncFixture) dispatch(t *testing.T, job *xasync.Job) error {
	t.Helper()
	object, err := f.host.FS.Object(context.Background(), job.EventURL)
	require.NoError(t, err)
	return f.server.DispatchStorageEvent(context.Background(), object)
}

func TestStandaloneAsyncHTTPDispatchStatusSQLite(t *testing.T) {
	f := newStandaloneAsync(t)
	f.start(t)
	token := f.jwt.Sign(t, time.Now().Add(time.Hour))
	keyless := httptest.NewRequest("POST", "/async-records?key=keyless", strings.NewReader(`{"data":{"id":2}}`))
	keyless.Header.Set("Authorization", "Bearer "+token)
	forbidden := httptest.NewRecorder()
	f.server.ServeHTTP(forbidden, keyless)
	require.Equal(t, 403, forbidden.Code)
	f.denied.Store(true)
	res := f.request("POST", "/async-records?key=denied", `{"data":{"id":2,"name":"denied"}}`, token)
	require.Equal(t, 403, res.Code, res.Body.String())
	f.denied.Store(false)
	for _, token := range []string{"invalid", f.jwt.Sign(t, time.Now().Add(-time.Hour)), ""} {
		res = f.request("POST", "/async-records?key=invalid", `{"data":{"id":2}}`, token)
		require.Equal(t, 401, res.Code, res.Body.String())
	}
	var count int
	require.NoError(t, f.jobDB.DB.QueryRow("SELECT count(*) FROM APP_JOBS").Scan(&count))
	require.Zero(t, count)
	job := f.schedule(t, "first", token)
	require.Equal(t, xasync.StatusPending, f.status(t, job.ID))
	require.Zero(t, f.calls.Load())
	var state string
	require.NoError(t, f.jobDB.DB.QueryRow("SELECT State FROM APP_JOBS WHERE ID=?", job.ID).Scan(&state))
	require.Contains(t, state, token)
	require.NotContains(t, state, "Initialized")
	// A different principal snapshot cannot override freshly verified input/current ACL.
	_, err := f.jobDB.DB.Exec("UPDATE APP_JOBS SET UserID='approved' WHERE ID=?", job.ID)
	require.NoError(t, err)
	f.denied.Store(true)
	require.Error(t, f.dispatch(t, job))
	require.Equal(t, xasync.StatusPending, f.status(t, job.ID))
	require.Zero(t, f.calls.Load())
	res = f.request("GET", "/async-status/"+job.ID, "", token)
	require.Equal(t, 403, res.Code, res.Body.String())
	f.denied.Store(false)
	f.replayDenied.Store(true)
	require.Error(t, f.dispatch(t, job))
	require.Equal(t, xasync.StatusPending, f.status(t, job.ID))
	require.Zero(t, f.calls.Load())
	f.replayDenied.Store(false)
	require.NoError(t, f.dispatch(t, job))
	require.Equal(t, xasync.StatusDone, f.status(t, job.ID))
	require.EqualValues(t, 1, f.calls.Load())
	require.NoError(t, f.dispatch(t, job))
	require.EqualValues(t, 1, f.calls.Load(), "duplicate must not rerun")
	res = f.request("GET", "/async-status/"+job.ID, "", token)
	require.Equal(t, 200, res.Code, res.Body.String())
	require.Contains(t, res.Body.String(), "COMPLETE")
	res = f.request("GET", "/async-status/"+job.ID, "", f.jwt.Sign(t, time.Now().Add(-time.Hour)))
	require.Equal(t, 401, res.Code, res.Body.String())
	f.DB.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT id,name FROM records ORDER BY id"}, []asyncrecords.Record{{ID: 1, Name: "first"}, {ID: 2, Name: "created"}})
	var expiry, end time.Time
	require.NoError(t, f.jobDB.DB.QueryRow("SELECT ExpiryTime,EndTime FROM APP_JOBS WHERE ID=?", job.ID).Scan(&expiry, &end))
	require.Equal(t, 120*time.Second, expiry.Sub(end))
	// Reader routes share the standalone SQL owner as well as the jobs owner.
	res = f.request("GET", "/async-read/1?key=reader-async", "", token)
	require.Equal(t, 200, res.Code, res.Body.String())
	var queued asyncrecords.ReadOutput
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &queued))
	require.Empty(t, queued.Rows)
	require.NoError(t, f.dispatch(t, queued.Job))
	require.Equal(t, xasync.StatusDone, f.status(t, queued.Job.ID))
	var query string
	require.NoError(t, f.jobDB.DB.QueryRow("SELECT SQLQuery FROM APP_JOBS WHERE ID=?", queued.Job.ID).Scan(&query))
	require.Contains(t, query, "records")
	res = f.request("GET", "/async-read/1?key=reader&wait=true", "", token)
	require.Equal(t, 200, res.Code, res.Body.String())
	require.Contains(t, res.Body.String(), "first")
	var reader asyncrecords.ReadOutput
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &reader))
	require.Equal(t, xasync.StatusDone, reader.Job.Status)
	for i := range f.authCalls {
		require.Positive(t, f.authCalls[i].Load())
	}
	require.NoError(t, f.server.Shutdown(context.Background()))
	require.Error(t, f.server.source.connections.SQL.DB.Ping())
	require.NoError(t, f.DB.DB.Ping())
	require.ErrorIs(t, f.dispatch(t, job), application.ErrClosed)
}

func TestStandaloneAsyncReplayRejectsStaleAndForgedJWTSQLite(t *testing.T) {
	for _, mode := range []string{"expired", "claims"} {
		t.Run(mode, func(t *testing.T) {
			f := newStandaloneAsync(t)
			f.start(t)
			token := f.jwt.Sign(t, time.Now().Add(time.Hour))
			job := f.schedule(t, mode, token)
			var state string
			require.NoError(t, f.jobDB.DB.QueryRow("SELECT State FROM APP_JOBS WHERE ID=?", job.ID).Scan(&state))
			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(state), &raw))
			if mode == "expired" {
				raw["JWT"], _ = json.Marshal("Bearer " + f.jwt.Sign(t, time.Now().Add(-time.Hour)))
			} else {
				raw["JWT"] = json.RawMessage(`{"sub":"approved","exp":9999999999}`)
			}
			changed, err := json.Marshal(raw)
			require.NoError(t, err)
			_, err = f.jobDB.DB.Exec("UPDATE APP_JOBS SET State=?,UserID='approved' WHERE ID=?", string(changed), job.ID)
			require.NoError(t, err)
			require.Error(t, f.dispatch(t, job))
			require.Zero(t, f.calls.Load())
			require.Zero(t, f.authCalls[1].Load(), "verifier must reject before host policy")
			require.Equal(t, xasync.StatusPending, f.status(t, job.ID))
		})
	}
}

func TestStandaloneAsyncWatcherRestartRejectsExpiredCredentialSQLite(t *testing.T) {
	f := newStandaloneAsync(t)
	f.start(t)
	expiry := time.Now().Add(2 * time.Second)
	job := f.schedule(t, "expires", f.jwt.Sign(t, expiry))
	require.NoError(t, f.server.Shutdown(context.Background()))
	time.Sleep(time.Until(expiry) + time.Second)
	f.cfg.JobURL = f.cfg.Jobs.Notification.Destination
	f.cfg.FailedJobURL = filepath.Join(f.Root, "failed")
	f.cfg.MaxJobs = 1
	f.cfg.Jobs.PollIntervalMs = 5
	reported := make(chan error, 1)
	f.host.WatchError = func(_ string, err error) {
		select {
		case reported <- err:
		default:
		}
	}
	f.start(t)
	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not reject expired JWT")
	}
	require.Eventually(t, func() bool {
		objects, err := f.host.FS.List(context.Background(), f.cfg.FailedJobURL, option.NewRecursive(true))
		if err != nil {
			return false
		}
		for _, object := range objects {
			if strings.HasSuffix(object.Name(), ".job") {
				return true
			}
		}
		return false
	}, 5*time.Second, 5*time.Millisecond)
	require.Equal(t, xasync.StatusPending, f.status(t, job.ID))
	require.Zero(t, f.calls.Load())
	require.NoError(t, f.server.Shutdown(context.Background()))
}

// Only the post-processing barrier is instrumented; all storage operations use AFS.
type joinedAsyncFS struct {
	afs.Service
	entered, release chan struct{}
	once             sync.Once
}

func (f *joinedAsyncFS) Delete(ctx context.Context, location string, opts ...storage.Option) error {
	if strings.HasSuffix(location, ".job") {
		f.once.Do(func() { close(f.entered) })
		<-f.release
	}
	return f.Service.Delete(ctx, location, opts...)
}

func TestStandaloneAsyncWatcherShutdownJoinsStorageSQLite(t *testing.T) {
	f := newStandaloneAsync(t)
	fs := &joinedAsyncFS{Service: f.host.FS, entered: make(chan struct{}), release: make(chan struct{})}
	f.host.FS = fs
	f.host.WatchError = func(source string, err error) { t.Logf("watch %s: %v", source, err) }
	f.cfg.JobURL = f.cfg.Jobs.Notification.Destination
	f.cfg.FailedJobURL = filepath.Join(f.Root, "failed")
	f.cfg.MaxJobs = 1
	f.cfg.Jobs.PollIntervalMs = 5
	notified := make(chan string, 1)
	f.host.Notify = func(ctx context.Context, j *xasync.Job) error { notified <- j.ID; return nil }
	f.start(t)
	// Always release blocked AFS before the server's cleanup joins workers.
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(fs.release) }) })
	job := f.schedule(t, "watched", f.jwt.Sign(t, time.Now().Add(time.Hour)))
	select {
	case <-fs.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not reach AFS acknowledgement")
	}
	select {
	case id := <-notified:
		require.Equal(t, job.ID, id)
	case <-time.After(time.Second):
		t.Fatal("missing terminal notification")
	}
	require.Equal(t, xasync.StatusDone, f.status(t, job.ID))
	require.EqualValues(t, 1, f.calls.Load())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, f.server.Shutdown(ctx), context.DeadlineExceeded)
	require.NoError(t, f.server.source.connections.SQL.DB.Ping(), "databases must outlive watcher acknowledgement")
	object, err := fs.Object(context.Background(), job.EventURL)
	require.NoError(t, err)
	require.ErrorIs(t, f.server.DispatchStorageEvent(context.Background(), object), application.ErrClosed)
	release.Do(func() { close(fs.release) })
	require.NoError(t, f.server.Shutdown(context.Background()))
	require.Error(t, f.server.source.connections.SQL.DB.Ping())
	exists, err := fs.Exists(context.Background(), job.EventURL)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestStandaloneAsyncRequiresTrustedHostBeforeOpeningStore(t *testing.T) {
	f := newStandaloneAsync(t)
	// Missing host policy must fail before any attempt to open even an invalid DB.
	f.cfg.Connectors[0].Driver = "not-a-driver"
	for _, host := range []*AsyncOptions{nil, {FS: afs.New()}} {
		_, err := New(context.Background(), Options{Config: f.cfg, Async: host})
		require.ErrorContains(t, err, "linked Async.Authorize")
	}
}

func TestStandaloneAsyncShutdownCancelsAndJoinsExecutionSQLite(t *testing.T) {
	f := newStandaloneAsync(t)
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	f.handlerGate = func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
	}
	f.start(t)
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	job := f.schedule(t, "cancel", f.jwt.Sign(t, time.Now().Add(time.Hour)))
	dispatched := make(chan error, 1)
	go func() {
		object, err := f.host.FS.Object(context.Background(), job.EventURL)
		if err != nil {
			dispatched <- err
			return
		}
		dispatched <- f.server.DispatchStorageEvent(context.Background(), object)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("execution did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, f.server.Shutdown(ctx), context.DeadlineExceeded)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("execution was not canceled")
	}
	require.NoError(t, f.server.source.connections.SQL.DB.Ping())
	once.Do(func() { close(release) })
	require.NoError(t, f.server.Shutdown(context.Background()))
	select {
	case err := <-dispatched:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("dispatch was not joined")
	}
	require.EqualValues(t, 1, f.calls.Load())
	require.Error(t, f.server.source.connections.SQL.DB.Ping())
	var count int
	require.NoError(t, f.DB.DB.QueryRow("SELECT count(*) FROM records").Scan(&count))
	require.Equal(t, 1, count)
}

func TestStandaloneAsyncStoreConfigurationSQLite(t *testing.T) {
	for _, mode := range []string{"unknown connector", "invalid table", "missing preprovisioned table", "native creation"} {
		t.Run(mode, func(t *testing.T) {
			f := newStandaloneAsync(t)
			switch mode {
			case "unknown connector":
				f.cfg.Jobs.Connector = "unknown"
			case "invalid table":
				f.cfg.Jobs.Table = "jobs; DROP TABLE records"
			case "missing preprovisioned table":
				f.cfg.Jobs.Table = "MISSING_JOBS"
			case "native creation":
				f.cfg.Jobs.Table = ""
				f.cfg.Jobs.DisableTableCreation = false
			}
			registry, err := asyncrecords.Exports(nil)
			require.NoError(t, err)
			s, err := New(context.Background(), Options{Config: f.cfg, Registry: registry, Async: &f.host})
			if mode != "native creation" {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NoError(t, s.Shutdown(context.Background()))
			rows, err := f.jobDB.DB.Query("PRAGMA table_info(DATLY_JOBS)")
			require.NoError(t, err)
			defer rows.Close()
			count := 0
			for rows.Next() {
				count++
			}
			require.NoError(t, rows.Err())
			require.Equal(t, 34, count)
			var mainCount int
			require.NoError(t, f.DB.DB.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='DATLY_JOBS'").Scan(&mainCount))
			require.Zero(t, mainCount)
		})
	}
}

func TestStandaloneAsyncRejectsExternalAdmissionWhileHTTPDrains(t *testing.T) {
	f := newStandaloneAsync(t)
	f.start(t)
	job := f.schedule(t, "drain", f.jwt.Sign(t, time.Now().Add(time.Hour)))
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	tracked := f.server.track(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { close(entered); <-release }))
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }); <-finished })
	go func() {
		defer close(finished)
		tracked.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, f.server.Shutdown(ctx), context.DeadlineExceeded)
	require.ErrorIs(t, f.dispatch(t, job), application.ErrClosed)
	require.Zero(t, f.calls.Load())
	require.Equal(t, xasync.StatusPending, f.status(t, job.ID))
	once.Do(func() { close(release) })
	require.NoError(t, f.server.Shutdown(context.Background()))
}

func TestStandaloneAsyncShutdownClosesAdmissionBeforeReturning(t *testing.T) {
	f := newStandaloneAsync(t)
	f.start(t)
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, f.server.Shutdown(ctx), context.Canceled)
	// An already-canceled caller must still close admission synchronously,
	// without waiting for the background cleanup goroutine to be scheduled.
	require.ErrorIs(t, f.server.DispatchStorageEvent(context.Background(), nil), application.ErrClosed)
	require.NoError(t, f.server.Shutdown(context.Background()))
}
