package application_test

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/afs/url"
	"github.com/viant/datly/application"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/jobs"
	xasync "github.com/viant/xdatly/async"
	xhandler "github.com/viant/xdatly/handler"
)

func TestManagerAsyncWatcherRetainsTerminalPersistenceFailureSQLite(t *testing.T) {
	f := &asyncAppFixture{}
	f.init(t, true)
	require.NoError(t, f.db.ExecStatements(context.Background(), `CREATE TABLE executed(id INTEGER)`))
	require.NoError(t, f.jobDB.ExecStatements(context.Background(), `CREATE TRIGGER fail_job_completion BEFORE UPDATE OF Status ON APP_JOBS WHEN NEW.Status IN ('DONE','ERROR') BEGIN SELECT RAISE(ABORT,'terminal write unavailable'); END`))
	var calls, notified atomic.Int32
	f.handler = rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
		calls.Add(1)
		value, _, err := inv.Binder.Lookup(ctx, xhandler.DataKey)
		if err != nil {
			return nil, err
		}
		return nil, value.(xhandler.Data).Execute(`INSERT INTO executed VALUES(1)`)
	})
	reported := make(chan error, 4)
	f.config.Watch.Error = func(_ string, err error) {
		select {
		case reported <- err:
		default:
		}
	}
	f.config.Notify = func(context.Context, *xasync.Job) error { notified.Add(1); return nil }
	f.start(t)
	job := f.schedule(t, context.Background())
	select {
	case err := <-reported:
		require.ErrorIs(t, err, jobs.ErrCompletionPending)
	case <-time.After(5 * time.Second):
		t.Fatal("missing persistence failure")
	}
	row, err := f.store.Get(context.Background(), job.Job.ID)
	require.NoError(t, err)
	require.Equal(t, xasync.StatusRunning, row.Status)
	require.Nil(t, row.EndTime)
	require.FileExists(t, url.Path(job.Job.EventURL))
	f.reload(t, 2)
	object, err := f.fs.Object(context.Background(), job.Job.EventURL)
	require.NoError(t, err)
	require.ErrorIs(t, f.manager.DispatchStorageEvent(context.Background(), object), jobs.ErrInProgress)
	require.NoError(t, f.manager.Shutdown(context.Background()))
	require.EqualValues(t, 1, calls.Load())
	require.Zero(t, notified.Load())
	var count int
	require.NoError(t, f.db.DB.QueryRow(`SELECT count(*) FROM executed`).Scan(&count))
	require.Equal(t, 1, count)
	require.FileExists(t, url.Path(job.Job.EventURL))
}

func TestManagerAsyncShutdownJoinsRetiredWatcherAndCurrentExternalExecutionSQLite(t *testing.T) {
	f := &asyncAppFixture{}
	f.init(t, true)
	first, second := make(chan context.Context, 1), make(chan context.Context, 1)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f.handler = rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
		first <- ctx
		<-release
		return nil, ctx.Err()
	})
	f.start(t)
	old := f.schedule(t, context.Background())
	var firstCtx context.Context
	select {
	case firstCtx = <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("watcher not executing")
	}
	f.handler = rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
		second <- ctx
		<-release
		return nil, ctx.Err()
	})
	f.reload(t, 2)
	next := f.schedule(t, context.Background())
	// MaxJobs=1 blocks another polling worker. Explicit delivery has its own
	// transport admission, but shares application cancellation and durable claims.
	object, err := f.fs.Object(context.Background(), next.Job.EventURL)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- f.manager.DispatchStorageEvent(context.Background(), object) }()
	var secondCtx context.Context
	select {
	case secondCtx = <-second:
	case <-time.After(5 * time.Second):
		t.Fatal("current external execution not started")
	}
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, f.manager.Shutdown(deadline), context.DeadlineExceeded)
	require.Eventually(t, func() bool { return firstCtx.Err() != nil && secondCtx.Err() != nil }, time.Second, time.Millisecond)
	_, err = f.manager.HandleJob(context.Background(), f.event(old))
	require.ErrorIs(t, err, application.ErrClosed)
	require.ErrorIs(t, f.manager.DispatchStorageEvent(context.Background(), object), application.ErrClosed)
	select {
	case <-done:
		t.Fatal("uncooperative execution was not joined")
	default:
	}
	close(release)
	require.NoError(t, f.manager.Shutdown(context.Background()))
	require.ErrorIs(t, <-done, context.Canceled)
	require.NoError(t, f.manager.Shutdown(deadline)) // completed result wins over old deadline
	for _, job := range []*jobs.Scheduled{old, next} {
		row, err := f.store.Get(context.Background(), job.Job.ID)
		require.NoError(t, err)
		require.Equal(t, xasync.StatusError, row.Status)
		require.NotNil(t, row.EndTime)
		require.FileExists(t, url.Path(job.Job.EventURL)) // canceled delivery retained
	}
}

func TestManagerAsyncShutdownJoinsAuthorizationAndNotificationSQLite(t *testing.T) {
	for _, phase := range []string{"submit", "replay", "notification"} {
		t.Run(phase, func(t *testing.T) {
			f := &asyncAppFixture{}
			f.init(t, phase != "submit")
			entered := make(chan context.Context, 1)
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			var notified atomic.Int32
			f.config.Authorize = func(ctx context.Context, access jobs.Access) error {
				if phase == "submit" && access.Action == jobs.Submit || phase == "replay" && access.Action == jobs.Replay {
					entered <- ctx
					<-release
				}
				return nil
			}
			f.config.Notify = func(ctx context.Context, _ *xasync.Job) error {
				notified.Add(1)
				if phase == "notification" {
					entered <- ctx
					<-release
				}
				return nil
			}
			f.start(t)
			done := make(chan error, 1)
			var job *jobs.Scheduled
			if phase == "submit" {
				go func() {
					_, err := f.manager.ScheduleJob(context.Background(), jobs.Submission{Job: xasync.Job{Request: xasync.Request{Method: "GET", URI: "/records"}}, Input: &struct{}{}})
					done <- err
				}()
			} else {
				job = f.schedule(t, context.Background())
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("operation not entered")
			}
			deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			require.ErrorIs(t, f.manager.Shutdown(deadline), context.DeadlineExceeded)
			close(release)
			require.NoError(t, f.manager.Shutdown(context.Background()))
			if phase == "submit" {
				require.ErrorIs(t, <-done, context.Canceled)
				return
			}
			row, err := f.store.Get(context.Background(), job.Job.ID)
			require.NoError(t, err)
			if phase == "replay" {
				require.Equal(t, xasync.StatusPending, row.Status)
				require.FileExists(t, url.Path(job.Job.EventURL))
				require.Zero(t, notified.Load())
				_, err = os.Stat(filepath.Join(f.config.Watch.FailedJobURL, time.Now().Format("20060102"), filepath.Base(url.Path(job.Job.EventURL))))
				require.True(t, os.IsNotExist(err))
			} else {
				require.Equal(t, xasync.StatusDone, row.Status)
				require.EqualValues(t, 1, notified.Load())
			}
		})
	}
}

func TestManagerAsyncWatcherMaxJobsSQLite(t *testing.T) {
	for _, max := range []int{0, 1, 2} {
		t.Run(string(rune('0'+max)), func(t *testing.T) {
			f := &asyncAppFixture{}
			f.init(t, true)
			f.config.Watch.MaxJobs = max
			entered := make(chan struct{}, 3)
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			var calls atomic.Int32
			f.handler = rhandler.HandlerFunc(func(ctx context.Context, _ rhandler.Invocation) (any, error) {
				calls.Add(1)
				entered <- struct{}{}
				select {
				case <-release:
					return nil, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			})
			f.start(t)
			for i := 0; i < 3; i++ {
				f.schedule(t, context.Background())
			}
			want := max
			if max == 0 {
				want = 3
			}
			for i := 0; i < want; i++ {
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("watcher worker missing")
				}
			}
			// List loops run repeatedly while workers are held. Positive MaxJobs bounds
			// accepted work; zero retains original unlimited watcher admission.
			require.Never(t, func() bool { return int(calls.Load()) > want }, 50*time.Millisecond, 5*time.Millisecond)
			close(release)
			require.Eventually(t, func() bool { return calls.Load() == 3 }, 5*time.Second, 5*time.Millisecond)
			require.NoError(t, f.manager.Shutdown(context.Background()))
		})
	}
}
