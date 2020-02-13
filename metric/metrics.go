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

func (m Metrics) Clone() *Metrics {
	var result = &Metrics{
		Queries: make([]*Query, 0),
		mux:     &sync.Mutex{},
	}
	for i := range m.Queries {
		query := *m.Queries[i]
		result.Queries = append(result.Queries, &query)
	}
	return result
}

//AddQuery adds query
func (m *Metrics) IncludeSQL() {
	if len(m.Queries) == 0 {
		return
	}
	for _, query := range m.Queries {
		query.Query = query.parametrizedSQL
	}
}

//NewMetrics creates a metrics
func NewMetrics() *Metrics {
	return &Metrics{
		mux: &sync.Mutex{},
	}
}
