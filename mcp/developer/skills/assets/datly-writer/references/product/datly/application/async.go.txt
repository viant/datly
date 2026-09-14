package application

import (
	"context"
	"fmt"
	"time"

	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	jobstorage "github.com/viant/datly/gateway/async"
	"github.com/viant/datly/runtime/jobs"
	xasync "github.com/viant/xdatly/async"
)

// AsyncConfig selects existing job and AFS services. Resolve Store once with
// bootstrap.JobStoreConfig.NewStore before New; its connector/table selection is
// fixed across reloads. The application never closes the caller-owned database.
// Callbacks and FS must be safe for concurrent use and remain valid until Shutdown
// completes. Authorize is mandatory; there is no implicit principal or policy.
type AsyncConfig struct {
	Store        *jobs.SQLStore
	Authorize    jobs.Authorizer
	TTL          time.Duration
	ErrorTTL     time.Duration
	Notify       func(context.Context, *xasync.Job) error
	FS           afs.Service
	Notification xasync.Notification
	// Empty JobURL disables polling, while schedule/status/external dispatch stay
	// available. Notification.Destination independently selects publication.
	Watch jobstorage.WatchConfig
}

// ScheduleJob captures against the generation current at admission. A durable
// pending row is replayed against metadata current at dispatch, even after reload.
// An operation already admitted finishes with its one pinned service.
func (m *Manager) ScheduleJob(ctx context.Context, request jobs.Submission) (*jobs.Scheduled, error) {
	ctx, current, release, err := m.admitJob(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return current.service.Schedule(ctx, request)
}

func (m *Manager) JobStatus(ctx context.Context, id string) (*xasync.Job, error) {
	ctx, current, release, err := m.admitJob(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return current.service.Status(ctx, id)
}

func (m *Manager) RepublishJob(ctx context.Context, id string) error {
	ctx, current, release, err := m.admitJob(ctx)
	if err != nil {
		return err
	}
	defer release()
	return current.service.Republish(ctx, id)
}

// HandleJob accepts original job event identity; the existing jobs service loads
// route/state authority from its durable row and reauthorizes replay.
func (m *Manager) HandleJob(ctx context.Context, event *jobs.Event) (any, error) {
	ctx, current, release, err := m.admitJob(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return current.service.HandleJob(ctx, event)
}

// DispatchStorageEvent owns admission through download, authorization, execution
// and terminal write-back. External delivery adapters own event acknowledgement;
// the managed Watcher owns acknowledgement for polling deliveries.
func (m *Manager) DispatchStorageEvent(ctx context.Context, object storage.Object) error {
	ctx, current, release, err := m.admitJob(ctx)
	if err != nil {
		return err
	}
	defer release()
	return current.dispatcher.DispatchStorageEvent(ctx, object)
}

func (m *Manager) admitJob(ctx context.Context) (context.Context, *asyncGeneration, func(), error) {
	return m.admitAsync(ctx, nil)
}

func (m *Manager) admitAsync(ctx context.Context, selected *asyncGeneration) (context.Context, *asyncGeneration, func(), error) {
	if m == nil || ctx == nil {
		return ctx, nil, nil, fmt.Errorf("application context is required")
	}
	m.publication.Lock()
	defer m.publication.Unlock()
	if err := ctx.Err(); err != nil {
		return ctx, nil, nil, err
	}
	if m.stopped.Load() {
		return ctx, nil, nil, ErrClosed
	}
	current := m.active.Load()
	if m.async == nil || current == nil || current.async == nil {
		return ctx, nil, nil, fmt.Errorf("application async services are not started")
	}
	// A caller may carry an old HTTP/MCP pin. A new job operation takes the
	// current generation, then keeps it for this entire accepted operation.
	if selected == nil {
		ctx = context.WithValue(ctx, generationKey{m}, current)
		selected = current.async
	}
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(m.async.ctx, cancel)
	m.async.work.Add(1)
	return ctx, selected, func() {
		stop()
		cancel()
		m.async.work.Done()
	}, nil
}
