package contract

import (
	"context"
	hstate "github.com/viant/xdatly/handler/state"
	"net/http"
)

type Dispatcher interface {
	Dispatch(ctx context.Context, path *Path, request *http.Request, form *hstate.Form) (interface{}, error)
}
