package reader

import "datly/base"

var _filters = base.NewFilters()

//Filters returns a reader filter singleton
func Filters() *base.Filters {
	return _filters
}
