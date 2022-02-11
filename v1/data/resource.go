package data

import (
	"github.com/viant/datly/v1/config"
)

type Resource struct {
	Connectors []*config.Connector
	Views      []*View
	References []*Reference
}
