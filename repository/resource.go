package repository

import (
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/view"
)

// Resources represents a resource
type Resources interface {
	AddResource(key string, aResource *view.Resource)
	Has(key string) bool
	Lookup(key string) (*version.Resource, error)
}
