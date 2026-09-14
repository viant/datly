package application_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	xasync "github.com/viant/xdatly/async"
)

func TestHTTPAsyncOneExecutionCompletionAndExporterOrderingSQLite(t *testing.T) {
	f := &httpAsyncFixture{}
	f.init(t, true)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f.config.Notify = func(context.Context, *xasync.Job) error { close(entered); <-release; return nil }
	exporter := &managedExport{}
	manager, err := application.New(nil, application.WithAsync(f.config), exporter.option())
	require.NoError(t, err)
	f.manager = manager
	defer manager.Shutdown(context.Background())
	require.NoError(t, manager.Reload(context.Background(), application.Request{Revision: 1, Compile: f.compile}))
	response := f.request("PATCH", "/inventory?id=7&key=observed", `{"quantity":0}`, "allowed")
	require.Equal(t, 200, response.Code, response.Body.String())
	var reply httpAsyncOutput
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &reply))
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal notification not reached")
	}
	status := f.request("GET", "/job-status/"+reply.Job.ID, "", "allowed")
	require.Equal(t, 200, status.Code, status.Body.String())
	require.NoError(t, manager.Reload(context.Background(), application.Request{Revision: 2, Compile: f.compile}))
	timeout, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, manager.Shutdown(timeout), context.DeadlineExceeded)
	require.Zero(t, exporter.stops.Load())
	exporter.onStop = func() {
		exists, err := f.fs.Exists(context.Background(), reply.Job.EventURL)
		if err != nil || exists {
			t.Errorf("exporter stopped before watcher cleanup: %v %v", exists, err)
		}
	}
	close(release)
	require.NoError(t, manager.Shutdown(context.Background()))
	require.EqualValues(t, 1, f.calls.Load())
	require.EqualValues(t, 1, exporter.stops.Load())
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	roots := 0
	for _, span := range exporter.spans {
		if !span.Parent().IsValid() {
			roots++
		}
	}
	require.Equal(t, 1, roots, "capture-only preflights must not emit completed handler invocations")
}
