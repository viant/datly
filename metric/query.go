package metric

import (
	"github.com/viant/datly/shared"
	"github.com/viant/dsc"
	"sync/atomic"
	"time"
)

//Query represents query metrics
type Query struct {
	parametrizedSQL *dsc.ParametrizedSQL
	SQL             *dsc.ParametrizedSQL `json:",omitempty"`
	DatView         string
	Count           uint32 `json:",omitempty"`
	CacheGetTimeMs  int    `json:",omitempty"`
	CacheHit        bool   `json:",omitempty"`
	CacheMiss       bool   `json:",omitempty"`
	ExecutionTimeMs int    `json:",omitempty"`
	FetchTimeMs     int    `json:",omitempty"`
	checkpoint      time.Time
}

func (q *Query) ParametrizedSQL() *dsc.ParametrizedSQL {
	return q.parametrizedSQL
}

func (q *Query) SetCacheGetTime(time time.Time) {
	q.CacheGetTimeMs = shared.ElapsedInMs(time)
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
func (q *Query) Increment() int {
	count := atomic.AddUint32(&q.Count, 1)
	if count == 1 {
		q.ExecutionTimeMs = shared.ElapsedInMs(q.checkpoint)
		q.checkpoint = time.Now()
	}
	return int(count)
}

//NewQuery returns new query
func NewQuery(dataView string, sql *dsc.ParametrizedSQL) *Query {
	return &Query{
		DatView:         dataView,
		checkpoint:      time.Now(),
		parametrizedSQL: sql,
	}
}
