package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	"github.com/viant/datly/runtime/jobs"
	xasync "github.com/viant/xdatly/async"
)

func TestHTTPCompletedReaderRelationsUseNativeEntriesSQLite(t *testing.T) {
	f := &readerResultsFixture{relations: true}
	f.init(t)
	require.NoError(t, f.db.ExecStatements(context.Background(), "CREATE TABLE result_children(parent_id INTEGER,name TEXT)", "INSERT INTO result_children VALUES(7,'cached child')"))
	f.start(t)
	job := f.schedule(t, "graph", 1)
	require.NoError(t, f.db.ExecStatements(context.Background(), "DROP TABLE result_children", "DROP TABLE result_records"))
	response := f.request("/read-results?id=7&tenant=1&key=graph")
	require.Equal(t, 200, response.Code, response.Body.String())
	var result readerResultOutput
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Len(t, result.Rows, 1)
	require.Len(t, result.Rows[0].Children, 1)
	require.Equal(t, "cached child", result.Rows[0].Children[0].Name)
	inspect := f.request("/read-job/" + job.ID + "?id=7&tenant=1")
	require.Equal(t, 200, inspect.Code, inspect.Body.String())
}

func TestHTTPCompletedReaderShutdownAndOneRootSQLite(t *testing.T) {
	f := &readerResultsFixture{}
	f.init(t)
	exporter := &managedExport{}
	manager, err := application.New(nil, application.WithAsync(f.config), exporter.option())
	require.NoError(t, err)
	f.manager = manager
	defer manager.Shutdown(context.Background())
	require.NoError(t, manager.Reload(context.Background(), application.Request{Revision: 1, Compile: f.compile}))
	job := f.schedule(t, "observed", 1)
	for _, path := range f.cacheFiles(t) {
		require.NoError(t, os.Remove(path))
	}
	fallback := f.request("/read-results?id=7&tenant=1&key=observed")
	require.Equal(t, 200, fallback.Code, fallback.Body.String())
	response := f.request("/read-results?id=7&tenant=1&key=observed")
	require.Equal(t, 200, response.Code, response.Body.String())
	status := f.request("/read-status/" + job.ID + "?tenant=1")
	require.Equal(t, 200, status.Code)
	entered, release := make(chan context.Context, 1), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f.gate = func(ctx context.Context) error { entered <- ctx; <-release; return ctx.Err() }
	require.NoError(t, manager.Reload(context.Background(), application.Request{Revision: 2, Compile: f.compile}))
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- f.request("/read-results?id=7&tenant=1&key=observed") }()
	var accepted context.Context
	select {
	case accepted = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("cached result read was not admitted")
	}
	f.gate = nil
	require.NoError(t, manager.Reload(context.Background(), application.Request{Revision: 3, Compile: f.compile}))
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, manager.Shutdown(deadline), context.DeadlineExceeded)
	require.ErrorIs(t, accepted.Err(), context.Canceled)
	require.Zero(t, exporter.stops.Load())
	close(release)
	require.NoError(t, manager.Shutdown(context.Background()))
	require.NotEqual(t, 200, (<-done).Code)
	record, err := f.store.Get(context.Background(), job.ID)
	require.NoError(t, err)
	require.Equal(t, xasync.StatusDone, record.Status)
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	roots := 0
	for _, span := range exporter.spans {
		if !span.Parent().IsValid() {
			roots++
		}
	}
	require.Equal(t, 4, roots, "one worker + one fallback read + one cached read + one canceled cached read; status/preflight are not execution roots")
}
func TestHTTPReaderTerminalErrorCannotBecomeCompletedResultSQLite(t *testing.T) {
	f := &readerResultsFixture{}
	f.init(t)
	require.NoError(t, f.jobDB.ExecStatements(context.Background(), `CREATE TRIGGER fail_reader_completion BEFORE UPDATE OF Status ON APP_JOBS WHEN NEW.Status IN ('DONE','ERROR') BEGIN SELECT RAISE(ABORT,'terminal persistence failed'); END`))
	reported := make(chan error, 1)
	f.config.Watch.Error = func(_ string, err error) {
		select {
		case reported <- err:
		default:
		}
	}
	f.start(t)
	response := f.request("/read-results?id=7&tenant=1&key=pending")
	require.Equal(t, 200, response.Code, response.Body.String())
	var result readerResultOutput
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	select {
	case err := <-reported:
		require.True(t, errors.Is(err, jobs.ErrCompletionPending), fmt.Sprint(err))
	case <-time.After(5 * time.Second):
		t.Fatal("terminal error missing")
	}
	before := f.reads.Load()
	lookup := f.request("/read-job/" + result.Job.ID + "?id=7&tenant=1")
	require.Equal(t, 409, lookup.Code, lookup.Body.String())
	require.Equal(t, before, f.reads.Load())
	again := f.request("/read-results?id=7&tenant=1&key=pending")
	require.Equal(t, 200, again.Code)
	require.Equal(t, before, f.reads.Load())
	exists, err := f.fs.Exists(context.Background(), result.Job.EventURL)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestHTTPCompletedReaderRefreshesDynamicCurrentSQLite(t *testing.T) {
	f := &readerResultsFixture{dynamic: true}
	f.init(t)
	require.NoError(t, f.db.ExecStatements(context.Background(), "CREATE TABLE result_filter(tenant INTEGER,allowed INTEGER)", "INSERT INTO result_filter VALUES(1,1)"))
	f.start(t)
	job := f.schedule(t, "dynamic", 1)
	record, err := f.store.Get(context.Background(), job.ID)
	require.NoError(t, err)
	// Poisoning historical dependency state has no effect: it is never a result
	// source. Actual Current is rebound through its independently scoped provider.
	var state map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(record.State), &state))
	state["Current"] = json.RawMessage(`{"Rows":[{"Permit":1}]}`)
	encoded, err := json.Marshal(state)
	require.NoError(t, err)
	_, err = f.jobDB.DB.Exec("UPDATE APP_JOBS SET State=? WHERE ID=?", string(encoded), job.ID)
	require.NoError(t, err)
	valid := f.request("/read-results?id=7&tenant=1&key=dynamic")
	require.Equal(t, 200, valid.Code, valid.Body.String())
	require.NoError(t, f.db.ExecStatements(context.Background(), "UPDATE result_filter SET allowed=0"))
	denied := f.request("/read-results?id=7&tenant=1&key=dynamic")
	require.Equal(t, 409, denied.Code, denied.Body.String())
	require.NotContains(t, denied.Body.String(), "cached tenant one")
}

func TestHTTPCompletedReaderDistinguishesEmptyHitAndExpiredJobSQLite(t *testing.T) {
	f := &readerResultsFixture{}
	f.init(t)
	f.start(t)
	response := f.request("/read-results?id=99&tenant=1&key=empty")
	require.Equal(t, 200, response.Code, response.Body.String())
	var output readerResultOutput
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &output))
	require.Eventually(t, func() bool {
		row, err := f.store.Get(context.Background(), output.Job.ID)
		return err == nil && row.Status == xasync.StatusDone
	}, 5*time.Second, 5*time.Millisecond)
	require.NoError(t, f.db.ExecStatements(context.Background(), "DROP TABLE result_records"))
	hit := f.request("/read-results?id=99&tenant=1&key=empty")
	require.Equal(t, 200, hit.Code, hit.Body.String())
	require.NoError(t, json.Unmarshal(hit.Body.Bytes(), &output))
	require.Empty(t, output.Rows)
	require.Equal(t, "COMPLETE", output.Code)
	_, err := f.jobDB.DB.Exec("UPDATE APP_JOBS SET ExpiryTime=? WHERE ID=?", time.Now().Add(-time.Minute), output.Job.ID)
	require.NoError(t, err)
	expired := f.request("/read-job/" + output.Job.ID + "?id=99&tenant=1")
	require.Equal(t, 410, expired.Code, expired.Body.String())
	status := f.request("/read-status/" + output.Job.ID + "?tenant=1")
	require.Equal(t, 200, status.Code)
}
