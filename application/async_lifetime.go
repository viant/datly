package application

import (
	"context"
	"fmt"
	"sync"

	"github.com/viant/afs"
	jobstorage "github.com/viant/datly/gateway/async"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/jobs"
)

// asyncLifetime owns only admission and lifetime. Watcher owns polling/work
// limits; jobs.Service owns durable transitions; Runtime owns canonical execution.
type asyncLifetime struct {
	config  jobs.Config
	fs      afs.Service
	watcher *jobstorage.Watcher
	ctx     context.Context
	cancel  context.CancelFunc
	work    sync.WaitGroup
	started bool // guarded by Manager.publication
}

type asyncGeneration struct {
	service    *jobs.Service
	dispatcher *jobstorage.Dispatcher
}

func (m *Manager) newAsync(config *AsyncConfig) (*asyncLifetime, error) {
	if config == nil {
		return nil, nil
	}
	if config.Store == nil || config.Authorize == nil {
		return nil, fmt.Errorf("application async requires a configured store and explicit authorization")
	}
	if config.TTL < 0 || config.ErrorTTL < 0 {
		return nil, fmt.Errorf("async expiry durations cannot be negative")
	}
	fs := config.FS
	if fs == nil {
		fs = afs.New()
	}
	publisher, err := jobstorage.NewPublisher(jobstorage.PublishConfig{FS: fs, Notification: config.Notification})
	if err != nil {
		return nil, err
	}
	// The polling dispatcher resolves Manager's current generation for each
	// accepted event. It never captures the runtime of the first publication.
	dispatcher, err := jobstorage.NewDispatcher(fs, m)
	if err != nil {
		return nil, err
	}
	watcher, err := jobstorage.NewWatcher(config.Watch, dispatcher)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &asyncLifetime{config: jobs.Config{Store: config.Store, Authorize: config.Authorize, Publisher: publisher, TTL: config.TTL, ErrorTTL: config.ErrorTTL, Notify: config.Notify}, fs: fs, watcher: watcher, ctx: ctx, cancel: cancel}, nil
}

func (a *asyncLifetime) generation(rt *runtime.Runtime) (*asyncGeneration, error) {
	if a == nil {
		return nil, nil
	}
	service, err := rt.NewAsyncService(a.config)
	if err != nil {
		return nil, err
	}
	dispatcher, err := jobstorage.NewDispatcher(a.fs, service)
	if err != nil {
		return nil, err
	}
	return &asyncGeneration{service: service, dispatcher: dispatcher}, nil
}

// start and stop are serialized with publication/admission by the Manager.
func (a *asyncLifetime) start() {
	if a == nil || a.started {
		return
	}
	a.started = true
	a.work.Add(1)
	go func() {
		defer a.work.Done()
		// A validated watcher only returns when disabled or canceled. Per-event
		// and listing errors are delivered by its configured Error callback.
		_ = a.watcher.Run(a.ctx)
	}()
}

func (a *asyncLifetime) stop() {
	if a != nil {
		a.cancel()
	}
}

func (a *asyncLifetime) wait() {
	if a != nil {
		a.work.Wait()
	}
}
