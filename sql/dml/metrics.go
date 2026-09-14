package dml

import (
	"context"
	"time"

	xexec "github.com/viant/xdatly/exec"
	"github.com/viant/xdatly/response"
)

// executionMetric follows original executor.logMetrics: it describes a native
// typed DML call, including a failed call, independently of transaction outcome.
// It never describes queued work or synthesizes SQL or transaction spans.
type executionMetric struct {
	context          *xexec.Context
	start            time.Time
	table, operation string
}

func (s executionStep) beginMetric(ctx context.Context) executionMetric {
	result := executionMetric{context: xexec.GetContext(ctx)}
	if result.context == nil {
		return result
	}
	result.table = s.table
	switch s.kind {
	case dataOpInsert:
		result.operation = "INSERT"
	case dataOpUpdate:
		result.operation = "UPDATE"
	case dataOpDelete:
		result.operation = "DELETE"
	}
	result.restart()
	return result
}

func (m *executionMetric) restart() {
	if m.context != nil {
		m.start = time.Now()
	}
}

func (m *executionMetric) complete(rows int64, err error) {
	if m.context == nil {
		return
	}
	// Publish a completed value once; subsequent native calls cannot mutate it.
	value := response.Metric{View: m.table, Type: m.operation, StartTime: m.start, EndTime: time.Now()}
	elapsed := value.EndTime.Sub(value.StartTime)
	value.ElapsedMs = int(elapsed.Milliseconds())
	value.Elapsed = elapsed.String()
	value.Rows = int(rows)
	if err != nil {
		value.Error = err.Error()
	}
	m.context.AppendMetrics(&value)
}
