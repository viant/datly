// Package jobs owns durable scheduling over the original Datly DATLY_JOBS table.
package jobs

import (
	"encoding/json"
	"fmt"
	xasync "github.com/viant/xdatly/async"
	xresponse "github.com/viant/xdatly/response"
	"time"
)

// Record adds the original persistence-only columns to the existing public SDK model.
// State is original session-cache JSON keyed by canonical parameter name.
// SQLQuery uses original Query/Args JSON objects. There is no StateCodec column.
type Record struct {
	xasync.Job
	State    string
	Metrics  string
	SQLQuery string
}

func (r *Record) Public() (*xasync.Job, error) {
	data, err := json.Marshal(r.Job)
	if err != nil {
		return nil, err
	}
	result := &xasync.Job{}
	err = json.Unmarshal(data, result)
	return result, err
}

func (r *Record) active(now time.Time) bool {
	return !r.Deactivated && (r.ExpiryTime == nil || r.ExpiryTime.After(now))
}
func (r *Record) finish(now time.Time, failure error, ttl, errorTTL time.Duration) {
	r.Status = xasync.StatusDone
	r.EndTime = &now
	r.RunTimeInMcs = int(now.Sub(*r.StartTime).Microseconds())
	if failure != nil {
		r.Status = xasync.StatusError
		message := failure.Error()
		r.Error = &message
		ttl = errorTTL
	}
	expiry := now.Add(ttl)
	r.ExpiryTime = &expiry
}
func (r *Record) validate() error {
	if r == nil || r.ID == "" || r.Method == "" || r.URI == "" || r.CreationTime.IsZero() {
		return fmt.Errorf("job identity, route and creation time are required")
	}
	if r.Status != xasync.StatusPending {
		return fmt.Errorf("new job must be PENDING")
	}
	return nil
}

// captureMetrics preserves original runtime metrics/SQLQuery columns using the
// existing SDK response data. It does not generate a second metrics stream.
func (r *Record) captureMetrics(metrics xresponse.Metrics) error {
	payload, err := json.Marshal(metrics)
	if err != nil {
		return err
	}
	r.Metrics = string(payload)
	var queries []*xresponse.ParametrizedSQL
	for _, metric := range metrics {
		if metric == nil {
			continue
		}
		for _, execution := range metric.Executions {
			if execution == nil {
				continue
			}
			queries = append(queries, &xresponse.ParametrizedSQL{Query: execution.SQL, Args: execution.Args})
			if r.CacheKey == nil && execution.CacheStats != nil {
				stats := execution.CacheStats
				r.CacheKey = &stats.Key
				r.CacheSet = &stats.Dataset
				r.CacheNamespace = &stats.Namespace
			}
		}
	}
	if len(queries) > 0 {
		payload, err = json.Marshal(queries)
		if err != nil {
			return err
		}
		r.SQLQuery = string(payload)
	}
	return nil
}
