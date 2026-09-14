package application_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	xasync "github.com/viant/xdatly/async"
)

func TestManagedAsyncNotificationAndWatcherJoinBeforeExporterSQLite(t *testing.T) {
	f := &asyncAppFixture{}
	f.init(t, true)
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	var completed atomic.Bool
	f.config.Notify = func(context.Context, *xasync.Job) error { close(entered); <-release; completed.Store(true); return nil }
	p := &managedExport{closed: make(chan struct{})}
	manager, err := application.New(nil, application.WithAsync(f.config), p.option())
	require.NoError(t, err)
	f.manager = manager
	t.Cleanup(func() { require.NoError(t, manager.Shutdown(context.Background())) })
	f.reload(t, 1)
	job := f.schedule(t, context.Background())
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("notification not entered")
	}
	f.reload(t, 2)
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, manager.Shutdown(deadline), context.DeadlineExceeded)
	require.Zero(t, p.stops.Load())
	p.onStop = func() {
		if !completed.Load() {
			t.Error("exporter closed before notification")
		}
		exists, err := f.fs.Exists(context.Background(), job.Job.EventURL)
		if err != nil || exists {
			t.Errorf("exporter closed before watcher post-processing: exists=%v err=%v", exists, err)
		}
	}
	close(release)
	// Deadline expiry does not require another Shutdown to finish the lifetime.
	select {
	case <-p.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not continue")
	}
	require.NoError(t, manager.Shutdown(context.Background()))
	require.EqualValues(t, 1, p.stops.Load())
}
