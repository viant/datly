package observability

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestRequestCompletionConcurrentAndIdempotent(t *testing.T) {
	route := spec.RouteRef{Method: "PATCH", Path: "/records/{id}"}
	policy := &Policy{Requests: []RequestObservation{{Route: route, Operation: OperationDescriptor{Name: "records.request", Location: "records", Description: "request"}}}}
	r := NewRecorder(nil, WithPolicy(policy))
	policy.Requests[0].Operation.Name = "mutated"
	require.Nil(t, r.BeginRequest(spec.RouteRef{Method: "GET", Path: "/records/{id}"}, time.Now()))
	var group sync.WaitGroup
	for i := 0; i < 64; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			start := time.Now()
			done := r.BeginRequest(route, start)
			done(start.Add(3*time.Millisecond), 400, false)
			done(start.Add(4*time.Millisecond), 200, false)
		}()
	}
	group.Wait()
	require.EqualValues(t, 64, r.Cumulative("records.request", "count"))
	require.EqualValues(t, 64, r.Cumulative("records.request", "Request"))
	require.EqualValues(t, 64, r.Cumulative("records.request", "Error"))
	require.EqualValues(t, 64, r.Cumulative("records.request", "status:4xx"))
	require.Zero(t, r.Cumulative("records.request", "Success"))
	require.Len(t, r.Values("records.request"), 7, "legacy provider includes implicit error key")
}
