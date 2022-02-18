package filter

import (
	"context"
	"github.com/viant/datly/v0/base/contract"
	"net/http"
)

//Filter request filter
type Filter func(ctx context.Context, request *contract.Request, writer http.ResponseWriter) (toContinue bool, err error)

//Filters represents filters
type Filters struct {
	Items []Filter
}

//Add adds a filter
func (f *Filters) Add(filter Filter) {
	f.Items = append(f.Items, filter)
}

//NewFilters create s filters
func NewFilters() *Filters {
	return &Filters{
		Items: make([]Filter, 0),
	}
}
