package observability

import (
	"sync"
	"time"

	"github.com/viant/datly/spec"
)

// HasRequestMetrics reports whether application policy maps any HTTP routes.
func (r *Recorder) HasRequestMetrics() bool {
	return r != nil && r.policy != nil && len(r.policy.requests) > 0
}

// BeginRequest keeps the legacy request provider on the shared gmetric service.
// The HTTP boundary supplies the committed status and any transport failure.
func (r *Recorder) BeginRequest(route spec.RouteRef, start time.Time) func(time.Time, int, bool) {
	if !r.HasRequestMetrics() {
		return nil
	}
	op, found := r.policy.requests[route.String()]
	if !found {
		return nil
	}
	done := r.Begin(op.Name, start)
	var once sync.Once
	return func(end time.Time, status int, failed bool) {
		once.Do(func() {
			values := []interface{}{"Request"}
			if failed || status >= 400 {
				values = append(values, "Error")
			} else {
				values = append(values, "Success")
			}
			switch {
			case status >= 200 && status < 300:
				values = append(values, "status:2xx")
			case status >= 400 && status < 500:
				values = append(values, "status:4xx")
			case status >= 500:
				values = append(values, "status:5xx")
			}
			done(end, values...)
		})
	}
}
