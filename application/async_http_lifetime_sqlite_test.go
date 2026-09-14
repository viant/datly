package application_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/afs/url"
	"github.com/viant/datly/application"
	"github.com/viant/datly/runtime/jobs"
	xasync "github.com/viant/xdatly/async"
)

func TestHTTPAsyncTerminalPersistenceRetentionSQLite(t *testing.T) {
	f := &httpAsyncFixture{}
	f.init(t, true)
	require.NoError(t, f.jobDB.ExecStatements(context.Background(), `CREATE TRIGGER fail_completion BEFORE UPDATE OF Status ON APP_JOBS WHEN NEW.Status IN ('DONE','ERROR') BEGIN SELECT RAISE(ABORT,'terminal write failed'); END`))
	reported := make(chan error, 2)
	f.config.Watch.Error = func(_ string, err error) {
		select {
		case reported <- err:
		default:
		}
	}
	f.start(t)
	res := f.request("PATCH", "/inventory?id=7&key=retained", `{"quantity":0}`, "allowed")
	require.Equal(t, 200, res.Code, res.Body.String())
	var output httpAsyncOutput
	require.NoError(t, json.Unmarshal(res.Body.Bytes(), &output))
	select {
	case err := <-reported:
		require.ErrorIs(t, err, jobs.ErrCompletionPending)
	case <-time.After(5 * time.Second):
		t.Fatal("terminal failure was not reported")
	}
	row, err := f.store.Get(context.Background(), output.Job.ID)
	require.NoError(t, err)
	require.Equal(t, xasync.StatusRunning, row.Status)
	require.Nil(t, row.EndTime)
	require.FileExists(t, url.Path(row.EventURL))
	require.EqualValues(t, 1, f.calls.Load())
	require.NoError(t, f.manager.Reload(context.Background(), application.Request{Revision: 2, Compile: f.compile}))
	duplicate := f.request("PATCH", "/inventory?id=7&key=retained", `{"quantity":999}`, "allowed")
	require.Equal(t, 200, duplicate.Code, duplicate.Body.String())
	object, err := f.fs.Object(context.Background(), row.EventURL)
	require.NoError(t, err)
	require.ErrorIs(t, f.manager.DispatchStorageEvent(context.Background(), object), jobs.ErrInProgress)
	status := f.request("GET", "/job-status/"+row.ID, "", "allowed")
	require.Equal(t, 200, status.Code, status.Body.String())
	require.Contains(t, status.Body.String(), `"code":"RUNNING"`)
	require.NoError(t, f.manager.Shutdown(context.Background()))
	require.EqualValues(t, 1, f.calls.Load())
	var quantity int
	require.NoError(t, f.db.DB.QueryRow("SELECT Quantity FROM inventory WHERE ID=7").Scan(&quantity))
	require.Zero(t, quantity)
	require.FileExists(t, url.Path(row.EventURL))
}

func TestHTTPAsyncCancellationDuringBodyCaptureSQLite(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "client"
		if shutdown {
			name = "shutdown"
		}
		t.Run(name, func(t *testing.T) {
			f := &httpAsyncFixture{}
			f.init(t, true)
			f.start(t)
			reader, writer := io.Pipe()
			defer writer.Close()
			entered := make(chan struct{})
			body := &signaledBody{ReadCloser: reader, entered: entered}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req := httptest.NewRequest("PATCH", "/inventory?id=7&key=cancel", body).WithContext(ctx)
			req.Header.Set("X-Key", "allowed")
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			done := make(chan struct{})
			go func() { defer close(done); f.manager.ServeHTTP(response, req) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("body capture did not begin")
			}
			if shutdown {
				require.NoError(t, f.manager.Shutdown(context.Background()))
			} else {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("body capture was orphaned")
			}
			require.NotEqual(t, 200, response.Code)
			var count int
			require.NoError(t, f.jobDB.DB.QueryRow("SELECT count(*) FROM APP_JOBS").Scan(&count))
			require.Zero(t, count)
			require.Zero(t, f.calls.Load())
		})
	}
}

type signaledBody struct {
	io.ReadCloser
	entered   chan struct{}
	announced bool
}

func (r *signaledBody) Read(data []byte) (int, error) {
	if !r.announced {
		r.announced = true
		close(r.entered)
	}
	return r.ReadCloser.Read(data)
}

func TestHTTPAsyncShutdownJoinsAcceptedAuthorizationSQLite(t *testing.T) {
	f := &httpAsyncFixture{}
	f.init(t, true)
	entered, release := make(chan context.Context, 1), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f.config.Authorize = func(ctx context.Context, access jobs.Access) error {
		if access.Action == jobs.Submit {
			entered <- ctx
			<-release
		}
		return nil
	}
	f.start(t)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- f.request("PATCH", "/inventory?id=7&key=slow", `{"quantity":0}`, "allowed") }()
	var accepted context.Context
	select {
	case accepted = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("authorization did not begin")
	}
	timeout, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, f.manager.Shutdown(timeout), context.DeadlineExceeded)
	require.ErrorIs(t, accepted.Err(), context.Canceled)
	close(release)
	require.NoError(t, f.manager.Shutdown(context.Background()))
	response := <-done
	require.NotEqual(t, 200, response.Code)
	require.NoError(t, f.manager.Shutdown(timeout))
	var count int
	require.NoError(t, f.jobDB.DB.QueryRow("SELECT count(*) FROM APP_JOBS").Scan(&count))
	require.Zero(t, count)
	require.Zero(t, f.calls.Load())
}

func TestHTTPAsyncReloadPinsWatcherAndShutdownSQLite(t *testing.T) {
	f := &httpAsyncFixture{}
	f.init(t, true)
	entered := make(chan context.Context, 1)
	f.handlerGate = func(ctx context.Context) error { entered <- ctx; <-ctx.Done(); return ctx.Err() }
	f.start(t)
	old := f.request("PATCH", "/inventory?id=7&key=old", `{"quantity":0}`, "allowed")
	require.Equal(t, 200, old.Code, old.Body.String())
	var operation context.Context
	select {
	case operation = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old generation did not execute")
	}
	f.handlerGate = nil
	require.NoError(t, f.manager.Reload(context.Background(), application.Request{Revision: 2, Compile: f.compile}))
	current := f.request("PATCH", "/inventory?id=7&key=new&wait=true", `{"note":"new generation"}`, "allowed")
	require.Equal(t, 200, current.Code, current.Body.String())
	require.Contains(t, current.Body.String(), "new generation")
	require.NoError(t, f.manager.Shutdown(context.Background()))
	require.ErrorIs(t, operation.Err(), context.Canceled)
	var result httpAsyncOutput
	require.NoError(t, json.Unmarshal(old.Body.Bytes(), &result))
	row, err := f.store.Get(context.Background(), result.Job.ID)
	require.NoError(t, err)
	require.Equal(t, xasync.StatusError, row.Status)
	require.FileExists(t, url.Path(row.EventURL))
}

func TestHTTPAsyncSyncRetryInvalidFormatAndMissingIDSQLite(t *testing.T) {
	f := &httpAsyncFixture{}
	f.init(t, true)
	f.start(t)
	bad := f.request("PATCH", "/inventory?id=7&key=bad&_format=unsupported", `{"quantity":0}`, "allowed")
	require.Equal(t, 400, bad.Code, bad.Body.String())
	require.Zero(t, f.calls.Load())
	var count int
	require.NoError(t, f.jobDB.DB.QueryRow("SELECT count(*) FROM APP_JOBS").Scan(&count))
	require.Zero(t, count)
	first := f.request("PATCH", "/inventory?id=7&key=once&wait=true", `{"quantity":0}`, "allowed")
	require.Equal(t, 200, first.Code, first.Body.String())
	second := f.request("PATCH", "/inventory?id=7&key=once&wait=true", `{"quantity":55}`, "allowed")
	require.Equal(t, 409, second.Code, second.Body.String())
	require.EqualValues(t, 1, f.calls.Load())
	missing := f.request("GET", "/job-status/missing", "", "allowed")
	require.Equal(t, 404, missing.Code, missing.Body.String())
	wrongMethod := f.request("DELETE", "/inventory?id=7&key=unknown", `{}`, "allowed")
	require.Equal(t, 405, wrongMethod.Code)
	require.NotEmpty(t, strings.TrimSpace(wrongMethod.Header().Get("Allow")))
}

func TestHTTPAsyncXLSForcesSynchronousSQLite(t *testing.T) {
	f := &httpAsyncFixture{}
	f.init(t, true)
	f.start(t)
	response := f.request("PATCH", "/inventory?id=7&key=xls&wait=false&_format=xls", `{"quantity":0}`, "allowed")
	require.Equal(t, 200, response.Code, response.Body.String())
	require.Contains(t, response.Header().Get("Content-Type"), "spreadsheetml")
	require.EqualValues(t, 1, f.calls.Load())
	var status, eventURL string
	require.NoError(t, f.jobDB.DB.QueryRow("SELECT Status,EventURL FROM APP_JOBS WHERE MatchKey=?", "Inventory/xls").Scan(&status, &eventURL))
	require.Equal(t, "DONE", status)
	exists, err := f.fs.Exists(context.Background(), eventURL)
	require.NoError(t, err)
	require.False(t, exists)
}
