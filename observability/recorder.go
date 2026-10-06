// Package observability owns application-scoped native Datly counters and logs.
// SQL/view capture remains in sql/reader; export consumes completed native records.
package observability

import (
	internallog "github.com/viant/datly/internal/logging"
	"sync"
	"time"

	"github.com/viant/gmetric"
	"github.com/viant/gmetric/counter"
	xlogger "github.com/viant/xdatly/logger"
	"github.com/viant/xdatly/response"
)

// Recorder retains original gmetric operation/window semantics. One recorder
// owns its service and serializes registration/update, as original gmetricx did.
type loggingState = internallog.State

type Recorder struct {
	*loggingState
	mu          sync.Mutex
	service     *gmetric.Service
	operations  map[string]*gmetric.Operation
	logger      xlogger.Logger
	readingData ReadingData
	policy      *compiledPolicy
	policyErr   error
}

func NewRecorder(logger xlogger.Logger, options ...RecorderOption) *Recorder {
	result := &Recorder{service: gmetric.New(), operations: map[string]*gmetric.Operation{}, logger: logger}
	for _, option := range options {
		if option != nil {
			option(result)
		}
	}
	return result
}
func (r *Recorder) operation(name string) *gmetric.Operation {
	if o := r.operations[name]; o != nil {
		return o
	}
	descriptor := OperationDescriptor{Name: name, Location: "datly", Description: "view performance", Provider: Native14}
	if r.policy != nil {
		if declared, exists := r.policy.operations[name]; exists {
			descriptor = declared
		}
	}
	o := r.service.MultiOperationCounter(descriptor.Location, name, descriptor.Description, time.Millisecond, time.Minute, 2, viewMetricProvider{source: descriptor.Provider == Source11})
	r.operations[name] = o
	return o
}
func (r *Recorder) Begin(name string, start time.Time) counter.OnDone {
	r.mu.Lock()
	op := r.operation(name)
	provider, _ := op.Provider.(viewMetricProvider)
	var sourceDone counter.OnDone
	if provider.source {
		sourceDone = op.Begin(start)
	}
	r.mu.Unlock()
	var completed bool
	var completedCount int64
	return func(end time.Time, values ...interface{}) int64 {
		r.mu.Lock()
		defer r.mu.Unlock()
		if sourceDone != nil {
			// Source gmetric starts total/recent Count before SQL. Retain that exact
			// owner/window completion through cancellation and overlapping generations.
			if completed {
				return completedCount
			}
			completed = true
			completedCount = sourceDone(end, values...)
			return completedCount
		}
		// Native default preserves its existing deferred Begin behavior.
		return r.operation(name).Begin(start)(end, values...)
	}
}
func (r *Recorder) Cache(name string, s *response.CacheStats) {
	if s == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	o := r.operation(name)
	switch {
	case s.ErrorType != "":
		o.IncrementValue("cache:error")
	case s.FoundWarmup:
		o.IncrementValue("cache:hit")
		o.IncrementValue("cache:warmup_hit")
	case s.FoundLazy:
		o.IncrementValue("cache:hit")
		o.IncrementValue("cache:lazy_hit")
	case s.Type == "write":
		o.IncrementValue("cache:miss")
		o.IncrementValue("cache:miss_write")
	default:
		o.IncrementValue("cache:miss")
	}
}

// Values returns detached native cumulative counter values, never row payloads.
func (r *Recorder) Values(name string) map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := map[string]int64{}
	if r.operations[name] == nil {
		return result
	}
	for _, key := range r.operations[name].Provider.Keys() {
		result[key] = r.service.LookupOperationCumulativeMetric(name, key)
	}
	return result
}

// Pending uses the original root-session aggregation, separately from each
// view's timed operation and status counters.
func (r *Recorder) Pending(name string, delta int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.operation(name).IncrementValueBy("Pending", delta)
}
func (r *Recorder) Cumulative(name, metric string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.service.LookupOperationCumulativeMetric(name, metric)
}
func (r *Recorder) Recent(name, metric string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.service.LookupOperationRecentMetric(name, metric)
}

// Created records successful native publications, independently of miss attempts.
func (r *Recorder) Created(name, kind string, entries int) {
	if entries <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	op := r.operation(name)
	if r.policy != nil {
		if descriptor, exists := r.policy.operations[name]; exists && descriptor.Provider == Source11 {
			return
		}
	}
	op.IncrementValueBy(cacheCreatedMetric, int64(entries))
	key := cacheLazyCreatedMetric
	if kind == "warmup" {
		key = cacheWarmupCreatedMetric
	}
	op.IncrementValueBy(key, int64(entries))
}
