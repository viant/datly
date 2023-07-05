package translator

import (
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
)

type Resource struct {
	Resource view.Resource
	Rule     *Rule
}

func (v *Resource) ExtractExplicitParameter(dSQL string) error {

	//	v.Resource.Parameters = TODO
	return nil
}

func (v *Resource) ExtractRouterOptions(dSQL string) error {
	return nil
}

func NewResource() *Resource {
	return &Resource{Rule: &Rule{Route: &router.Route{}}}
}
