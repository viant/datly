package engine

import (
	"context"
	"sync"

	xexec "github.com/viant/xdatly/exec"
	xresponse "github.com/viant/xdatly/response"
)

type responseWriter struct {
	mu      sync.RWMutex
	ctx     *xexec.Context
	status  int
	errors  []error
	metrics xresponse.Metrics
}

func newResponseWriter(ctx context.Context) *responseWriter {
	return &responseWriter{ctx: xexec.GetContext(ctx)}
}

func (w *responseWriter) StatusCode() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

func (w *responseWriter) SetStatusCode(code int) {
	w.mu.Lock()
	w.status = code
	w.mu.Unlock()
	if w.ctx != nil {
		w.ctx.StatusCode = code
	}
}

func (w *responseWriter) AddError(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	w.errors = append(w.errors, err)
	w.mu.Unlock()
	if w.ctx != nil {
		w.ctx.SetError(err)
	}
}

func (w *responseWriter) AddMetric(metric *xresponse.Metric) {
	if metric == nil {
		return
	}
	w.mu.Lock()
	w.metrics = append(w.metrics, metric)
	w.mu.Unlock()
	if w.ctx != nil {
		w.ctx.AppendMetrics(metric)
	}
}

func (w *responseWriter) Metrics() xresponse.Metrics {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return append(xresponse.Metrics(nil), w.metrics...)
}
