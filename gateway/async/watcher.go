package async

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/afs/option"
	afsstorage "github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/datly/runtime/jobs"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type WatchConfig struct {
	JobURL       string
	FailedJobURL string
	MaxJobs      int
	PollInterval time.Duration
	// Error receives dispatch/post-processing failures. Concurrent workers may
	// report concurrently; the sink must be concurrency-safe.
	Error func(string, error)
}
type Watcher struct {
	config     WatchConfig
	dispatcher *Dispatcher
	running    atomic.Bool
}

func NewWatcher(config WatchConfig, dispatcher *Dispatcher) (*Watcher, error) {
	if dispatcher == nil {
		return nil, fmt.Errorf("job watcher requires a storage dispatcher")
	}
	if config.MaxJobs < 0 || config.PollInterval < 0 {
		return nil, fmt.Errorf("invalid job watcher limits")
	}
	if config.JobURL != "" && config.FailedJobURL == "" {
		return nil, fmt.Errorf("FailedJobURL is required when JobURL is configured")
	}
	if config.JobURL != "" && url.Equals(config.JobURL, config.FailedJobURL) {
		return nil, fmt.Errorf("JobURL and FailedJobURL must differ")
	}
	if config.PollInterval == 0 {
		config.PollInterval = 100 * time.Millisecond
	}
	if config.JobURL != "" {
		config.JobURL = url.Normalize(config.JobURL, "file")
	}
	if config.FailedJobURL != "" {
		config.FailedJobURL = url.Normalize(config.FailedJobURL, "file")
	}
	return &Watcher{config: config, dispatcher: dispatcher}, nil
}

// Run joins all owned workers on server-context cancellation. MaxJobs=0 retains
// original unlimited admission; a positive value is scoped to this watcher.
func (w *Watcher) Run(ctx context.Context) error {
	if w.config.JobURL == "" {
		return nil
	}
	if !w.running.CompareAndSwap(false, true) {
		return fmt.Errorf("job watcher is already running")
	}
	defer w.running.Store(false)
	var workers sync.WaitGroup
	defer workers.Wait()
	pending := map[string]bool{}
	var mu sync.Mutex
	var limiter chan struct{}
	if w.config.MaxJobs > 0 {
		limiter = make(chan struct{}, w.config.MaxJobs)
	}
	timer := time.NewTicker(w.config.PollInterval)
	defer timer.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		objects, err := w.dispatcher.fs.List(ctx, w.config.JobURL, option.NewRecursive(true))
		if err != nil {
			w.report(w.config.JobURL, err)
		}
		for _, object := range objects {
			if object.IsDir() || !strings.HasSuffix(url.Path(object.URL()), ".job") || strings.HasPrefix(url.Normalize(object.URL(), "file"), strings.TrimRight(w.config.FailedJobURL, "/")+"/") {
				continue
			}
			mu.Lock()
			active := pending[object.URL()]
			if !active {
				pending[object.URL()] = true
			}
			mu.Unlock()
			if active {
				continue
			}
			if limiter != nil {
				select {
				case limiter <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if err := ctx.Err(); err != nil {
				if limiter != nil {
					<-limiter
				}
				return err
			}
			workers.Add(1)
			go func(object afsstorage.Object) {
				defer workers.Done()
				defer func() {
					if limiter != nil {
						<-limiter
					}
					mu.Lock()
					delete(pending, object.URL())
					mu.Unlock()
				}()
				w.handle(ctx, object)
			}(object)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
}
func (w *Watcher) handle(ctx context.Context, object afsstorage.Object) {
	err := w.dispatcher.DispatchStorageEvent(ctx, object)
	if errors.Is(err, jobs.ErrCompletionPending) {
		w.report(object.URL(), err)
		return
	}
	if errors.Is(err, jobs.ErrInProgress) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err == nil {
		err = w.dispatcher.fs.Delete(cleanup, object.URL())
	} else {
		w.report(object.URL(), err)
		destination := url.Join(w.config.FailedJobURL, time.Now().Format("20060102"), object.Name())
		exists, checkErr := w.dispatcher.fs.Exists(cleanup, destination)
		if checkErr != nil {
			err = checkErr
		} else if exists {
			err = fmt.Errorf("failed job destination already exists: %s", destination)
		} else {
			err = w.dispatcher.fs.Move(cleanup, object.URL(), destination)
		}
	}
	if err != nil {
		w.report(object.URL(), err)
	}
}
func (w *Watcher) report(source string, err error) {
	if w.config.Error != nil {
		w.config.Error(source, err)
	}
}
