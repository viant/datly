package reader

import (
	"github.com/viant/datly/data"
	rdata "github.com/viant/toolbox/data"
)

type Upstream struct {
	Params    rdata.Map
	Collector *data.Collector
}
