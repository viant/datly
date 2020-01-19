package metric

import "github.com/viant/dsc"

//Query represents query metrics
type Query struct {
	*dsc.ParametrizedSQL
	Count           int  `json:",omitempty"`
	CacheGetTimeMs  int  `json:",omitempty"`
	CacheHit        bool `json:",omitempty"`
	ExecutionTimeMs int  `json:",omitempty"`
	FetchTimeMs     int  `json:",omitempty"`
}

//NewQuery returns new query
func NewQuery(sql *dsc.ParametrizedSQL) *Query {
	return &Query{
		ParametrizedSQL: sql,
	}
}
