package reader

import (
	"github.com/viant/datly/v0/filter"
)

var _filters = filter.NewFilters()

//Filters returns a reader filter singleton
func Filters() *filter.Filters {
	return _filters
}
