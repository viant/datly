package contract

import (
	"context"
	"github.com/viant/datly/view/state"
	"net/http"
)

type Dispatcher interface {
	Dispatch(ctx context.Context, path *Path, request *http.Request, form *state.Form) (interface{}, error)
}
