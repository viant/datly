package translator

import (
	"github.com/viant/cloudless/async/mbus"
	"github.com/viant/datly/gateway/runtime/standalone"
	"github.com/viant/datly/view"
)

type Repository struct {
	config       *standalone.Config
	resource     *Resource
	connections  view.Connectors
	constants    []*view.Parameter
	caches       view.Caches
	messageBuses []*mbus.Resource
}
