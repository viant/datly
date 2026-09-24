package runtime

import (
	"context"
	"errors"
	"fmt"
	dexec "github.com/viant/datly/exec"
	"sync"

	"github.com/viant/datly/observability"
	"github.com/viant/datly/observability/otel"
	xlogger "github.com/viant/xdatly/logger"
)

type ObservabilityConfig struct {
	Logger      xlogger.Logger
	ReadingData observability.ReadingData
	OTel        *otel.Config
}

// Observability is application-owned. Native capture is always present; the
// optional adapter has no request-scoped lifecycle or global SDK registration.
type Observability struct {
	Recorder    *observability.Recorder
	adapter     *otel.Adapter
	mu          sync.Mutex
	stopped     bool
	active      sync.WaitGroup
	drained     chan struct{}
	finishing   bool
	shutdownErr error
}

func WithObservability(config ObservabilityConfig) Option {
	return func(options *options) error {
		if options.managedObservability != nil {
			return fmt.Errorf("application owns observability configuration")
		}
		options.observabilityConfigured = true
		copy := config
		if config.OTel != nil {
			export := *config.OTel
			copy.OTel = &export
		}
		options.observability = copy
		return nil
	}
}
func (r *Runtime) Observability() *Observability {
	if r == nil {
		return nil
	}
	return r.observability
}

// NewObservability creates one application-scoped capture/export owner.
func NewObservability(config ObservabilityConfig) (*Observability, error) {
	o := &Observability{Recorder: observability.NewRecorder(config.Logger, observability.WithReadingData(config.ReadingData))}
	if err := o.initialize(config); err != nil {
		return nil, err
	}
	return o, nil
}
func (o *Observability) initialize(config ObservabilityConfig) error {
	o.drained = make(chan struct{})
	if config.OTel == nil {
		return nil
	}
	var err error
	o.adapter, err = otel.New(*config.OTel)
	return err
}

// WithManagedObservability borrows an application owner. It rejects stage-local
// policies rather than creating an exporter per reloadable generation.
func WithManagedObservability(owner *Observability) Option {
	return func(options *options) error {
		if owner == nil {
			return fmt.Errorf("managed observability is required")
		}
		if options.observabilityConfigured || options.managedObservability != nil && options.managedObservability != owner {
			return fmt.Errorf("application owns observability configuration")
		}
		options.managedObservability = owner
		return nil
	}
}

type observationInvocationKey struct{ owner *Observability }

func (o *Observability) begin(ctx context.Context) (context.Context, bool) {
	key := observationInvocationKey{o}
	if ctx.Value(key) != nil {
		return ctx, false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.stopped {
		return ctx, false
	}
	o.active.Add(1)
	return context.WithValue(ctx, key, true), true
}

// Stop ends export-lifetime admission. Existing native invocations retain their
// behavior, and accepted invocations finish capture before export is drained.
func (o *Observability) Stop() {
	if o == nil || o.adapter == nil {
		return
	}
	o.mu.Lock()
	o.stopped = true
	o.mu.Unlock()
}

// Shutdown continues cleanup after a caller deadline; there is one join/drain
// task per owner, never a task per generation or per retry.
func (o *Observability) Shutdown(ctx context.Context) error {
	if o == nil || o.adapter == nil {
		return nil
	}
	o.mu.Lock()
	o.stopped = true
	if !o.finishing {
		o.finishing = true
		go func() { o.active.Wait(); o.shutdownErr = o.adapter.Shutdown(context.Background()); close(o.drained) }()
	}
	o.mu.Unlock()
	select {
	case <-o.drained:
		return o.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Shutdown releases only a standalone runtime's owned services: the default
// outbound client registry it created and, when not managed, observability.
// Managed generations borrow their application owner and must not close it
// on retirement; explicitly configured client providers are likewise borrowed.
func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	var err error
	if r.ownsClients && r.clients != nil {
		err = r.clients.Close()
	}
	if !r.ownsObservability {
		return err
	}
	return errors.Join(err, r.observability.Shutdown(ctx))
}
func (o *Observability) ExportStats() otel.Stats {
	if o == nil {
		return otel.Stats{}
	}
	return o.adapter.Stats()
}

// observedReader wires application services into a detached registered execution.
type observedReader interface {
	WithRecorder(*observability.Recorder) dexec.Reader
}
