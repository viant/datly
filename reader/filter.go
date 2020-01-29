package reader

import "github.com/viant/datly/filter"

var _filters = filter.NewFilters()

//Filters returns a reader filter singleton
func Filters() *filter.Filters {
	return _filters
}
