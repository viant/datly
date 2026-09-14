package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	jobstorage "github.com/viant/datly/gateway/async"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/jobs"
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx"
	xasync "github.com/viant/xdatly/async"
	xhandler "github.com/viant/xdatly/handler"
)

type asyncAppFixture struct {
	handler rhandler.Handler
	reloadFixture
	jobDB   *sqlite.Harness
	store   *jobs.SQLStore
	config  application.AsyncConfig
	fs      afs.Service
	manager *application.Manager
}

func (f *asyncAppFixture) init(t *testing.T, watch bool) {
	t.Helper()
	f.reloadFixture.init(t)
	f.jobDB = sqlite.New(t)
	connectors := &dsql.SQLComponent{DB: f.db.DB}
	require.NoError(t, connectors.RegisterConnector("app", f.db.DB))
	require.NoError(t, connectors.RegisterConnector("jobs", f.jobDB.DB))
	selection := bootstrap.JobStoreConfig{SQL: connectors, Connector: "jobs", Table: "APP_JOBS", Dataset: "main"}
	var err error
	f.store, err = selection.NewStore(context.Background())
	require.NoError(t, err)
	// Later bootstrap carrier changes cannot redirect the already selected store.
	selection.Connector, selection.Table = "app", "elsewhere"
	root := t.TempDir()
	source, failed := filepath.Join(root, "jobs"), filepath.Join(root, "failed")
	require.NoError(t, os.MkdirAll(source, 0700))
	f.fs = afs.New()
	f.config = application.AsyncConfig{Store: f.store, FS: f.fs, Authorize: func(context.Context, jobs.Access) error { return nil }, Notification: xasync.Notification{Method: xasync.NotificationMethodStorage, Destination: source}}
	if watch {
		f.config.Watch = jobstorage.WatchConfig{JobURL: source, FailedJobURL: failed, MaxJobs: 1, PollInterval: 5 * time.Millisecond}
	}
}

func (f *asyncAppFixture) start(t *testing.T) {
	t.Helper()
	var err error
	f.manager, err = application.New(nil, application.WithAsync(f.config))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.manager.Shutdown(context.Background())) })
	f.reload(t, 1)
}

func (f *asyncAppFixture) reload(t *testing.T, revision int) {
	t.Helper()
	require.NoError(t, f.manager.Reload(context.Background(), application.Request{Revision: uint64(revision), Compile: f.compile(revision)}))
}

func (f *asyncAppFixture) schedule(t *testing.T, ctx context.Context) *jobs.Scheduled {
	t.Helper()
	job, err := f.manager.ScheduleJob(ctx, jobs.Submission{Job: xasync.Job{Request: xasync.Request{Method: "GET", URI: "/records"}, MainView: "Records"}, Input: &struct{}{}})
	require.NoError(t, err)
	return job
}

func (f *asyncAppFixture) event(job *jobs.Scheduled) *jobs.Event {
	return &jobs.Event{Job: *job.Job}
}

func TestManagerAsyncReloadPinsAcceptedWorkSQLite(t *testing.T) {
	f := &asyncAppFixture{}
	f.block = true
	f.init(t, false)
	defer func() {
		select {
		case <-f.release:
		default:
			close(f.release)
		}
	}()
	f.start(t)
	ctx := context.Background()
	pinned, _, err := f.manager.Pin(ctx)
	require.NoError(t, err)
	old, pending := f.schedule(t, ctx), f.schedule(t, ctx)
	object, err := f.fs.Object(ctx, old.Job.EventURL)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- f.manager.DispatchStorageEvent(ctx, object) }()
	select {
	case <-f.started:
	case <-time.After(5 * time.Second):
		t.Fatal("old generation did not execute")
	}
	f.reload(t, 2)
	// The already accepted execution stays in generation 1; pending/new work uses
	// generation 2, including a submitter carrying an older HTTP/MCP context pin.
	fresh := f.schedule(t, pinned)
	require.Contains(t, fresh.Plan.SQL, "name AS value")
	result, err := f.manager.HandleJob(pinned, f.event(pending))
	require.NoError(t, err)
	encoded, err := json.Marshal(result)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"value":"new"`)
	_, err = f.manager.HandleJob(ctx, f.event(old))
	require.ErrorIs(t, err, jobs.ErrInProgress)
	close(f.release)
	require.NoError(t, <-done) // nested reader also verifies nested generation parity
	state, err := f.manager.JobStatus(ctx, old.Job.ID)
	require.NoError(t, err)
	require.Equal(t, xasync.StatusDone, state.Status)
	_, err = f.manager.HandleJob(ctx, f.event(old))
	require.NoError(t, err) // durable completion does not reexecute old reader
	require.NoError(t, f.manager.RepublishJob(ctx, fresh.Job.ID))
	_, err = f.manager.HandleJob(ctx, f.event(fresh))
	require.NoError(t, err)
	require.ErrorIs(t, f.manager.RepublishJob(ctx, fresh.Job.ID), jobs.ErrTransition)
}

func TestManagerAsyncWatcherStartupAndReloadSQLite(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			f := &asyncAppFixture{}
			f.init(t, true)
			var completions atomic.Int32
			f.config.Notify = func(ctx context.Context, job *xasync.Job) error {
				row, err := f.store.Get(ctx, job.ID)
				if err != nil {
					return err
				}
				if row.EndTime == nil {
					return errors.New("notification before durable completion")
				}
				completions.Add(1)
				return nil
			}
			option := application.WithAsync(f.config)
			// Both the option and the owner detach caller's scalar configuration.
			source, failed := f.config.Watch.JobURL, f.config.Watch.FailedJobURL
			f.config.Watch.JobURL = filepath.Join(t.TempDir(), "wrong")
			manager, err := application.New(nil, option)
			require.NoError(t, err)
			f.manager = manager
			t.Cleanup(func() { require.NoError(t, manager.Shutdown(context.Background())) })
			_, err = manager.ScheduleJob(context.Background(), jobs.Submission{})
			require.Error(t, err)
			bad := filepath.Join(source, "invalid.job")
			require.NoError(t, os.WriteFile(bad, []byte("invalid"), 0600))
			require.Error(t, manager.Reload(context.Background(), application.Request{Revision: 3, Compile: f.compile(3)}))
			require.FileExists(t, bad)
			require.Equal(t, uint64(0), manager.Revision())
			f.reload(t, 1)
			require.Eventually(t, func() bool { _, err := os.Stat(bad); return os.IsNotExist(err) }, 5*time.Second, 5*time.Millisecond)
			require.FileExists(t, filepath.Join(failed, time.Now().Format("20060102"), "invalid.job"))
			f.reload(t, 2)
			if fail {
				require.NoError(t, f.db.ExecStatements(context.Background(), "DROP TABLE records"))
			}
			job := f.schedule(t, context.Background())
			require.Eventually(t, func() bool { _, err := os.Stat(url.Path(job.Job.EventURL)); return os.IsNotExist(err) }, 5*time.Second, 5*time.Millisecond)
			state, err := manager.JobStatus(context.Background(), job.Job.ID)
			require.NoError(t, err)
			want := xasync.StatusDone
			if fail {
				want = xasync.StatusError
				require.FileExists(t, filepath.Join(failed, time.Now().Format("20060102"), filepath.Base(url.Path(job.Job.EventURL))))
			}
			require.Equal(t, want, state.Status)
			require.EqualValues(t, 1, completions.Load())
			require.NoError(t, manager.Shutdown(context.Background()))
			_, err = manager.JobStatus(context.Background(), job.Job.ID)
			require.ErrorIs(t, err, application.ErrClosed)
		})
	}
}

func TestManagerAsyncAuthorizationAndDurableAuthoritySQLite(t *testing.T) {
	f := &asyncAppFixture{}
	f.init(t, false)
	var denied atomic.Bool
	denied.Store(true)
	policyError := errors.New("access denied")
	var submits, replays, inspects atomic.Int32
	f.config.Authorize = func(_ context.Context, access jobs.Access) error {
		switch access.Action {
		case jobs.Submit:
			submits.Add(1)
		case jobs.Replay:
			replays.Add(1)
		case jobs.Inspect:
			inspects.Add(1)
		}
		if denied.Load() {
			return policyError
		}
		return nil
	}
	f.start(t)
	_, err := f.manager.ScheduleJob(context.Background(), jobs.Submission{Job: xasync.Job{Request: xasync.Request{Method: "GET", URI: "/records"}}, Input: &struct{}{}})
	require.ErrorIs(t, err, policyError)
	denied.Store(false)
	job := f.schedule(t, context.Background())
	denied.Store(true)
	_, err = f.manager.JobStatus(context.Background(), job.Job.ID)
	require.ErrorIs(t, err, policyError)
	require.ErrorIs(t, f.manager.RepublishJob(context.Background(), job.Job.ID), policyError)
	_, err = f.manager.HandleJob(context.Background(), f.event(job))
	require.ErrorIs(t, err, policyError)
	row, err := f.store.Get(context.Background(), job.Job.ID)
	require.NoError(t, err)
	require.Equal(t, xasync.StatusPending, row.Status)
	denied.Store(false)
	forged := f.event(job)
	forged.URI = "/different"
	_, err = f.manager.HandleJob(context.Background(), forged)
	require.Error(t, err)
	forged = f.event(job)
	forged.State = `{"other":"untrusted"}`
	forged.SQL = json.RawMessage(`"DELETE FROM records"`)
	_, err = f.manager.HandleJob(context.Background(), forged)
	require.NoError(t, err)
	require.Positive(t, submits.Load())
	require.Positive(t, replays.Load())
	require.Positive(t, inspects.Load())
}

func TestManagerAsyncConfigurationRequiresExplicitServicesSQLite(t *testing.T) {
	f := &asyncAppFixture{}
	f.init(t, true)
	for _, change := range []func(*application.AsyncConfig){
		func(c *application.AsyncConfig) { c.Authorize = nil },
		func(c *application.AsyncConfig) { c.Store = nil },
		func(c *application.AsyncConfig) { c.Notification.Destination = "" },
		func(c *application.AsyncConfig) { c.Watch.FailedJobURL = "" },
		func(c *application.AsyncConfig) { c.Watch.MaxJobs = -1 },
		func(c *application.AsyncConfig) { c.TTL = -time.Second },
	} {
		config := f.config
		change(&config)
		_, err := application.New(nil, application.WithAsync(config))
		require.Error(t, err)
	}
}

// Preserve the real planner when the shared nested-generation probe wraps a reader.
func (r *nestedReader) PlanRead(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (*exec.ReadPlan, error) {
	return r.Reader.(exec.ReadPlanner).PlanRead(ctx, input, binder, resolver)
}

func (f *asyncAppFixture) compile(revision int) func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
	compile, handler := f.reloadFixture.compile(revision), f.handler
	return func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		built, err := compile(ctx, types)
		if err != nil {
			return nil, err
		}
		if handler != nil {
			for _, component := range built.Components {
				if component.Component.Name == "Records" {
					component.Handler = handler
					component.DataSource = sqldml.Source{DB: f.db.DB}
				}
			}
		}
		return built, nil
	}
}
