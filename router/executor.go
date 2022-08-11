package router

import (
	"context"
	"github.com/viant/datly/executor"
	"net/http"
)

func (r *Router) executorHandler(route *Route) viewHandler {
	return func(response http.ResponseWriter, request *http.Request) {
		err := r.executorHandlerWithError(route, request)

		if err != nil {
			r.writeErr(response, route, err, 400)
			return
		}

		response.WriteHeader(200)
	}
}

func (r *Router) executorHandlerWithError(route *Route, request *http.Request) error {
	ctx := context.Background()

	selectors, err := CreateSelectorsFromRoute(ctx, route, request, route.Index._viewDetails...)
	if err != nil {
		return err
	}

	session, err := executor.NewSession(selectors, route.View)
	if err != nil {
		return err
	}

	anExecutor := executor.New()

	return anExecutor.Exec(ctx, session)
}
