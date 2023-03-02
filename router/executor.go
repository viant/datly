package router

import (
	"context"
	"github.com/viant/datly/executor"
	"net/http"
)

func (r *Router) executorHandler(route *Route) viewHandler {
	return func(response http.ResponseWriter, request *http.Request) {
		body, err := r.executorHandlerWithError(route, request)

		if err != nil {
			r.writeErr(response, route, err, 400)
			return
		}

		response.WriteHeader(200)
		if len(body) > 0 {
			_, _ = response.Write(body)
		}
	}
}

func (r *Router) executorHandlerWithError(route *Route, request *http.Request) ([]byte, error) {
	ctx := context.Background()

	parameters, err := NewRequestParameters(request, route)
	if err != nil {
		return nil, err
	}

	selectors, _, err := CreateSelectorsFromRoute(ctx, route, request, parameters, route.Index._viewDetails...)
	if err != nil {
		return nil, err
	}

	session, err := executor.NewSession(selectors, route.View)
	if err != nil {
		return nil, err
	}

	anExecutor := executor.New()

	err = anExecutor.Exec(ctx, session)
	if err != nil || route.ResponseBody == nil {
		return nil, err
	}

	body, err := route.execResponseBody(parameters, session)
	if err != nil {
		return nil, err
	}

	responseBody := r.wrapWithResponseIfNeeded(body, route, nil, nil)
	return route._outputMarshaller.Marshal(responseBody, nil)
}

func (r *Route) execResponseBody(parameters *RequestParams, session *executor.Session) (interface{}, error) {
	if r.ResponseBody != nil {
		return r.ResponseBody.getValue(session)
	}

	return parameters.RequestBody()
}
