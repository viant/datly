package metric

import "sync"

//Metrics represents metrics
type Metrics struct {
	Queries []*Query `json:",omitempty"`
	mux     *sync.Mutex
}

//AddQuery adds query
func (m *Metrics) AddQuery(query *Query) {
	m.mux.Lock()
	defer m.mux.Unlock()
	if len(m.Queries) == 0 {
		m.Queries = make([]*Query, 0)
	}
	m.Queries = append(m.Queries, query)
}

//NewMetrics creates a metrics
func NewMetrics() *Metrics {
	return &Metrics{
		mux: &sync.Mutex{},
	}
}
