package metric

import (
	"github.com/viant/datly/shared"
	"github.com/viant/dsc"
	"sync/atomic"
	"time"
)

//Query represents query metrics
type Query struct {
	*dsc.ParametrizedSQL
	Count           uint32 `json:",omitempty"`
	CacheGetTimeMs  int    `json:",omitempty"`
	CacheHit        bool   `json:",omitempty"`
	ExecutionTimeMs int    `json:",omitempty"`
	FetchTimeMs     int    `json:",omitempty"`
	checkpoint      time.Time
}

//SetFetchTime sets fetch time
func (q *Query) SetExecutionTime() {
	q.ExecutionTimeMs = shared.ElapsedInMs(q.checkpoint)
}

//SetFetchTime sets fetch time
func (q *Query) SetFetchTime() {
	q.FetchTimeMs = shared.ElapsedInMs(q.checkpoint)
}

//Increment increments record count
func (q *Query) Increment() {
	if atomic.AddUint32(&q.Count, 1) == 1 {
		q.ExecutionTimeMs = shared.ElapsedInMs(q.checkpoint)
		q.checkpoint = time.Now()
	}
}

//NewQuery returns new query
func NewQuery(sql *dsc.ParametrizedSQL) *Query {
	return &Query{
		checkpoint:      time.Now(),
		ParametrizedSQL: sql,
	}
}
