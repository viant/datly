package contract

import (
	"context"
	"net/http"
)

type Dispatcher interface {
	Dispatch(ctx context.Context, path *Path, request *http.Request) (interface{}, error)
}
