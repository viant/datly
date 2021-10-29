package router

import "github.com/viant/datly/oas/openapi3"

type route struct {
	path string
	item *openapi3.PathItem

	connect     *operation
	delete      *operation
	get         *operation
	head        *operation
	options     *operation
	patch       *operation
	post        *operation
	put         *operation
	trace       *operation

}

func (r *route) Init() {

}

func (r *route) Validate() error {
	return nil
}