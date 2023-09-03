package resolver

import (
	"context"
	"github.com/viant/datly/repository/component"
	"net/http"
)

type Dispatcher interface {
	Dispatch(ctx context.Context, path *component.Path, request *http.Request) (interface{}, error)
}
