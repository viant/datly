package router

import (
	"context"
	"github.com/viant/datly/executor"
	"github.com/viant/datly/router/async"
	"net/http"
)

func (r *Router) executorHandler(route *Route) viewHandler {
	return func(response http.ResponseWriter, request *http.Request, _ *async.Record) {
		successCode, body, err := r.executorHandlerWithError(route, request)

		if err != nil {
			r.writeErr(response, route, err, 400)
			return
		}

		if successCode >= 300 || successCode < 200 {
			successCode = 200
		}

		response.WriteHeader(successCode)
		if len(body) > 0 {
			_, _ = response.Write(body)
		}
	}
}

func (r *Router) executorHandlerWithError(route *Route, request *http.Request) (int, []byte, error) {
	ctx := context.Background()

	parameters, err := NewRequestParameters(request, route)
	statusCode := -1
	if err != nil {
		return statusCode, nil, err
	}

	selectors, _, err := CreateSelectorsFromRoute(ctx, route, request, parameters, route.Index._viewDetails...)
	if err != nil {
		return statusCode, nil, err
	}

	session, err := executor.NewSession(selectors, route.View)
	if err != nil {
		return statusCode, nil, err
	}

	anExecutor := executor.New()

	err = anExecutor.Exec(ctx, session)
	if err != nil || route.ResponseBody == nil {
		return statusCode, nil, err
	}

	body, err := route.execResponseBody(parameters, session)
	if err != nil {
		return statusCode, nil, err
	}

	responseBody := r.wrapWithResponseIfNeeded(body, route, nil, nil, session.State)
	marshal, err := route._marshaller.Marshal(responseBody)
	if session.State.ResponseBuilder.ResponseCode != 0 {
		statusCode = session.State.ResponseBuilder.ResponseCode
	}

	return statusCode, marshal, err
}

func (r *Route) execResponseBody(parameters *RequestParams, session *executor.Session) (interface{}, error) {
	if r.ResponseBody != nil {
		return r.ResponseBody.getValue(session)
	}

	return parameters.BodyParameter(nil)
}
