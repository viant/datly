package translator

import (
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/view"
)

type Repository struct {
	config      *standalone.Config
	connections view.Connectors
	literals    []view.Parameter
}
