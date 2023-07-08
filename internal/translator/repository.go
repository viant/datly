package translator

import (
	"github.com/viant/cloudless/async/mbus"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/view"
)

type Repository struct {
	Config       *standalone.Config
	Resource     []*Resource
	Connections  view.Connectors
	Constants    []*view.Parameter
	Caches       view.Caches
	MessageBuses []*mbus.Resource
	Warnings     []*Warning
}
