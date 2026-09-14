package observability

import (
	"github.com/viant/xdatly/response"
	"time"
)

// ReadingData is the explicitly configured synchronous query callback from
// original Datly. View identifies the view for application filtering. Arguments
// are borrowed and read-only during the call; callbacks must be concurrency safe.
// This trusted local callback can see SQL arguments. It is not an OTel exporter;
// it must not perform exporter/network work on the request path.
type ReadingData func(view string, duration time.Duration, sql string, read int, params []any, err error)

type RecorderOption func(*Recorder)

// WithReadingData configures a recorder at construction; nil disables callbacks.
func WithReadingData(callback ReadingData) RecorderOption {
	return func(r *Recorder) { r.readingData = callback }
}

// QueryRead invokes the callback only after QueryAll returned. Preparation,
// planning, and panic paths do not emit ReadingData, matching the original owner.
func (r *Recorder) QueryRead(view string, execution *response.SQLExecution, read int, err error) {
	if r == nil || r.readingData == nil || execution == nil {
		return
	}
	r.readingData(view, execution.EndTime.Sub(execution.StartTime), execution.SQL, read, execution.Args, err)
}
