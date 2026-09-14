package otel

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type Adapter struct {
	config                              Config
	mu                                  sync.Mutex
	closed                              bool
	queue                               chan Completion
	slots                               chan struct{}
	done                                chan struct{}
	cancel                              context.CancelFunc
	shutdownErr                         error
	accepted, dropped, exported, failed atomic.Uint64
	mapper                              *mapper
	provider                            *sdktrace.TracerProvider
	admissions                          sync.WaitGroup
}

func New(config Config) (*Adapter, error) {
	if !config.Enabled {
		return nil, nil
	}
	if config.Exporter == nil {
		return nil, fmt.Errorf("OTel exporter is required when enabled")
	}
	if config.BatchSize < 0 || config.BatchTimeout < 0 || config.MaxSpans < 0 || config.QueueSize < 0 || config.ExportTimeout < 0 {
		return nil, fmt.Errorf("OTel queue size and export timeout must not be negative")
	}
	if config.MaxSpans == 0 {
		config.MaxSpans = 2048
	}
	if config.QueueSize == 0 {
		config.QueueSize = 256
	}
	if config.BatchSize == 0 {
		config.BatchSize = 64
	}
	if config.BatchSize > config.QueueSize {
		config.BatchSize = config.QueueSize
	}
	if config.BatchTimeout == 0 {
		config.BatchTimeout = 5 * time.Millisecond
	}
	if config.ExportTimeout == 0 {
		config.ExportTimeout = 10 * time.Second
	}
	a := &Adapter{config: config, queue: make(chan Completion, config.QueueSize), slots: make(chan struct{}, config.QueueSize), done: make(chan struct{})}
	processor := &spanBatch{}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithIDGenerator(mappedIDs{}), sdktrace.WithSpanProcessor(processor), sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", config.ServiceName), attribute.String("service.version", config.ServiceVersion))))
	a.provider = provider
	a.mapper = &mapper{tracer: provider.Tracer("github.com/viant/datly/observability"), batch: processor, includeSQL: config.IncludeSQL}
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel
	go a.run(ctx)
	return a, nil
}

// TrySubmit reserves bounded capacity before detaching native records. Overflow
// drops export only. Completed native records remain owned by the invocation.
// The caller must have joined all writers before completing this invocation.
func (a *Adapter) TrySubmit(c Completion) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	if a.closed || c.Context == nil {
		a.mu.Unlock()
		a.dropped.Add(1)
		return false
	}
	select {
	case a.slots <- struct{}{}:
	default:
		a.mu.Unlock()
		a.dropped.Add(1)
		return false
	}
	a.admissions.Add(1)
	a.mu.Unlock()
	defer a.admissions.Done()
	count := 1
	if c.Context.Trace != nil && len(c.Context.Trace.Spans) > 1 {
		count = len(c.Context.Trace.Spans)
	}
	for _, m := range c.Context.Metrics {
		if m != nil {
			count += 1 + len(m.Executions)
		}
		if count > a.config.MaxSpans {
			a.dropped.Add(1)
			<-a.slots
			return false
		}
	}
	if count > a.config.MaxSpans || len(c.Links) > 128 {
		a.dropped.Add(1)
		<-a.slots
		return false
	}
	c = c.detached(a.config.IncludeSQL)
	a.accepted.Add(1)
	a.queue <- c
	return true
}
func (a *Adapter) run(ctx context.Context) {
	defer func() {
		if recover() != nil {
			a.shutdownErr = fmt.Errorf("OTel worker shutdown panic")
		}
		close(a.done)
	}()
	batch := make([]Completion, 0, a.config.BatchSize)
	for {
		first, open := <-a.queue
		if !open {
			break
		}
		batch = append(batch[:0], first)
		closed := false
		timer := time.NewTimer(a.config.BatchTimeout)
	fill:
		for len(batch) < a.config.BatchSize {
			select {
			case c, ok := <-a.queue:
				if !ok {
					closed = true
					break fill
				}
				batch = append(batch, c)
			case <-timer.C:
				break fill
			case <-ctx.Done():
				break fill
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		a.export(ctx, batch)
		for index := range batch {
			batch[index] = Completion{}
			<-a.slots
		}
		if closed {
			break
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.config.ExportTimeout)
	defer cancel()
	_ = a.provider.Shutdown(shutdownCtx)
	a.shutdownErr = a.config.Exporter.Shutdown(shutdownCtx)
}
func (a *Adapter) export(ctx context.Context, batch []Completion) {
	remaining := uint64(len(batch))
	defer func() {
		if recover() != nil {
			a.failed.Add(remaining)
		}
	}()
	if ctx.Err() != nil {
		a.failed.Add(remaining)
		return
	}
	var spans []sdktrace.ReadOnlySpan
	valid := uint64(0)
	for _, c := range batch {
		converted, err := a.mapper.convert(c)
		if err != nil {
			a.failed.Add(1)
			remaining--
			continue
		}
		spans = append(spans, converted...)
		valid++
	}
	if valid == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, a.config.ExportTimeout)
	defer cancel()
	if err := a.config.Exporter.ExportSpans(ctx, spans); err != nil {
		a.failed.Add(valid)
		return
	}
	a.exported.Add(valid)
}

// Shutdown stops admission and drains accepted work. On deadline it cancels the
// worker's exporter context; exporters must honor context cancellation.
func (a *Adapter) Shutdown(ctx context.Context) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	if !a.closed {
		a.closed = true
		go func() { a.admissions.Wait(); close(a.queue) }()
	}
	a.mu.Unlock()
	select {
	case <-a.done:
		a.cancel()
		return a.shutdownErr
	case <-ctx.Done():
		a.cancel()
		return ctx.Err()
	}
}
func (a *Adapter) Stats() Stats {
	if a == nil {
		return Stats{}
	}
	return Stats{Accepted: a.accepted.Load(), Dropped: a.dropped.Load(), Exported: a.exported.Load(), Failed: a.failed.Load()}
}
