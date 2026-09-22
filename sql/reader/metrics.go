package reader

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/viant/datly/data"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/sql/reader/collector"
	"github.com/viant/gmetric/counter"
	"github.com/viant/sqlx/io/read/cache"
	xexec "github.com/viant/xdatly/exec"
	"github.com/viant/xdatly/response"
)

// viewRead adapts original reader.NewExecutionInfo/afterRead to invocation-local
// records. Completed records are appended once; no exporter runs in this owner.
type viewRead struct {
	collector   *collector.Collector
	session     *Session
	metric      *response.Metric
	scope       string
	ctx         context.Context
	counterDone counter.OnDone
	mu          sync.Mutex
	completed   bool
	ids         map[string]bool
}

func (s *Session) initMetrics(owner *observability.Recorder) {
	if owner == nil {
		owner = observability.NewRecorder(nil)
	}
	s.recorder = owner
	if s.metricScope == "" && s.Component != nil {
		s.metricScope = s.Component.Key.String()
	}
	s.Metrics = nil
	s.pendingScope = s.metricScope
	if s.Artifact != nil && s.Artifact.Root != nil {
		s.pendingScope += "/" + viewName(s.Artifact.Root.View)
	}
}
func (s *Session) beginView(ctx context.Context, v *data.View) *viewRead {
	start := time.Now()
	name := viewName(v)
	scope := name
	if s.metricScope != "" {
		scope = s.metricScope + "/" + name
	}
	r := &viewRead{session: s, ctx: ctx, scope: scope, metric: &response.Metric{ID: uuid.NewString(), View: name, Type: "SELECT", StartTime: start}}
	r.counterDone = s.recorder.Begin(scope, start)
	return r
}
func (r *viewRead) done(rows int, err error) {
	r.mu.Lock()
	if r.completed {
		r.mu.Unlock()
		return
	}
	r.completed = true
	r.metric.EndTime = time.Now()
	elapsed := r.metric.EndTime.Sub(r.metric.StartTime)
	r.metric.Elapsed = elapsed.String()
	r.metric.ElapsedMs = int(elapsed.Milliseconds())
	if rows < 0 && r.collector != nil {
		rows = r.collector.Len()
	}
	if rows < 0 {
		rows = 0
		for _, e := range r.metric.Executions {
			rows += e.Rows
		}
	}
	r.metric.Rows = rows
	status := "Success"
	if err != nil {
		r.metric.Error = err.Error()
		status = "Error"
	}
	r.mu.Unlock()
	r.counterDone(r.metric.EndTime, status)
	s := r.session
	s.metricsMu.Lock()
	s.Metrics = append(s.Metrics, r.metric)
	s.metricsMu.Unlock()
	traceID := ""
	if ec := xexec.GetContext(r.ctx); ec != nil {
		ec.AppendMetrics(r.metric)
		traceID = ec.TraceID
	}
	s.recorder.Read(traceID, r.metric)
}
func (r *viewRead) execution(query *cache.ParmetrizedQuery, id, parent string) *response.SQLExecution {
	if id == "" {
		id = uuid.NewString()
	}
	now := time.Now()
	e := &response.SQLExecution{ID: id, ParentID: parent, StartTime: now, EndTime: now, SQL: query.SQL, Args: query.Args}
	r.mu.Lock()
	if r.ids == nil {
		r.ids = map[string]bool{}
	}
	if r.ids[e.ID] {
		e.ID = uuid.NewString()
	}
	r.ids[e.ID] = true
	r.metric.Executions = append(r.metric.Executions, e)
	r.mu.Unlock()
	return e
}
func (r *viewRead) completeSQL(e *response.SQLExecution, stats *cache.Stats, rows int, err error) {
	e.EndTime = time.Now()
	e.Rows = rows
	e.SetError(err)
	if stats != nil {
		e.CacheStats = &response.CacheStats{Type: string(stats.Type), RecordsCounter: stats.RecordsCounter, Key: stats.Key, Dataset: stats.Dataset, Namespace: stats.Namespace, FoundWarmup: stats.FoundWarmup, FoundLazy: stats.FoundLazy, ErrorType: stats.ErrorType, ErrorCode: int(stats.ErrorCode)}
		if stats.ExpiryTime != nil {
			expiry := *stats.ExpiryTime
			e.CacheStats.ExpiryTime = &expiry
		}
		e.CacheStats.SetCreatedTime(stats.CreatedTime)
		if err == nil && stats.Type == cache.TypeWrite && stats.CreatedTime != nil {
			r.session.recorder.Created(r.scope, "lazy", 1)
		}
		r.session.recorder.Cache(r.scope, e.CacheStats)
	}
	traceID := ""
	if ec := xexec.GetContext(r.ctx); ec != nil {
		traceID = ec.TraceID
	}
	r.session.recorder.SQL(traceID, r.metric.View, e)
}

var errReadPanic = errors.New("reader panicked")

// finish observes an unwinding panic but preserves the caller's panic contract.
func (r *viewRead) finish(err *error) {
	failure := *err
	panicked := recover()
	if panicked != nil {
		failure = errReadPanic
	}
	r.done(-1, failure)
	if panicked != nil {
		panic(panicked)
	}
}
