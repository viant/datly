package reader

import (
	"github.com/viant/datly/base/contract"
)

//Request represents read request
type Request struct {
	contract.Request
	DataOnly bool //flag to return data and status, errors section to the client
}
