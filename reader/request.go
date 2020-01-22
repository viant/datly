package reader

import (
	"github.com/viant/datly/base"
)

//Request represents read request
type Request struct {
	base.Request
	DataOnly bool //flag to return data and status, errors section to the client
}
