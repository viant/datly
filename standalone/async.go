package standalone

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/viant/afs"
	"github.com/viant/afs/storage"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	jobstorage "github.com/viant/datly/gateway/async"
	"github.com/viant/datly/runtime/jobs"
	xasync "github.com/viant/xdatly/async"
)

// AsyncOptions is the trusted linked host integration for configured Jobs.
// Authorize must check current application access for every action, including
// status. Canonical inputs have verified declared codecs; storage Inspect has
// nil Input and requires independently trusted worker authority. Stored
// principals are not authority.
// Nil authorization rejects startup. Callbacks and FS must be concurrency-safe
// and remain valid until Shutdown completes; the host retains FS ownership.
type AsyncOptions struct {
	Authorize  jobs.Authorizer
	FS         afs.Service
	Notify     func(context.Context, *xasync.Job) error
	WatchError func(string, error)
}

func (s *source) async(ctx context.Context, host *AsyncOptions, logger *slog.Logger) (*application.AsyncConfig, error) {
	j := s.config.Jobs
	if j == nil {
		return nil, nil
	}
	store, err := (bootstrap.JobStoreConfig{SQL: s.connections.SQL, Connector: j.Connector, Table: j.Table, Dataset: j.Dataset, DisableTableCreation: j.DisableTableCreation}).NewStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("standalone Jobs store: %w", err)
	}
	report := host.WatchError
	if report == nil {
		// Do not log persisted state, credentials or credential-bearing cloud URLs.
		report = func(_ string, _ error) { logger.Error("datly async watch operation failed") }
	}
	return &application.AsyncConfig{
		Store: store, Authorize: host.Authorize, FS: host.FS, Notify: host.Notify,
		Notification: j.Notification, TTL: time.Duration(j.TTLSeconds) * time.Second,
		ErrorTTL: time.Duration(j.ErrorTTLSeconds) * time.Second,
		Watch:    jobstorage.WatchConfig{JobURL: s.config.JobURL, FailedJobURL: s.config.FailedJobURL, MaxJobs: s.config.MaxJobs, PollInterval: time.Duration(j.PollIntervalMs) * time.Millisecond, Error: report},
	}, nil
}

// DispatchStorageEvent admits externally delivered AFS objects through the same
// Manager used by polling and HTTP. The delivery adapter owns acknowledgement.
func (s *Server) DispatchStorageEvent(ctx context.Context, object storage.Object) error {
	if s == nil {
		return fmt.Errorf("standalone server is required")
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return application.ErrClosed
	}
	return s.manager.DispatchStorageEvent(ctx, object)
}
