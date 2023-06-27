package expand

import (
	"github.com/viant/datly/view/keywords"
	"github.com/viant/velty/functions"
)

var fnNop = keywords.AddAndGet("Nop", &functions.Entry{
	Metadata: &keywords.StandaloneFn{},
	Handler:  nil,
})

type noper struct {
}

func (n noper) Nop(any ...interface{}) string {
	return ""
}
