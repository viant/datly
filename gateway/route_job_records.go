package gateway

import (
	"context"
	"fmt"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/gateway/router/async"
	"github.com/viant/datly/gateway/router/marshal/json"
	httputils2 "github.com/viant/datly/utils/httputils"
	"net/http"
)

func NewJobRecords(URL string, jobIDParam string, routers []*router.Router, apiKeys []*router.APIKey, marshaller *json.Marshaller, match func(method, URL string, req *http.Request) (*Route, error)) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			URL:    URL,
			Method: http.MethodGet,
		},
		ApiKeys: apiKeys,
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			records, err := findAllJobRecords(ctx, req, routers, URL, jobIDParam, match)
			if err != nil {
				httputils2.WriteError(response, err)
				return
			}

			marshal, err := marshaller.Marshal(records)
			if err != nil {
				httputils2.WriteError(response, httputils2.NewHttpStatusError(http.StatusInternalServerError))
				return
			}

			_, _ = response.Write(marshal)
		},
	}
}

func findAllJobRecords(ctx context.Context, req *http.Request, routers []*router.Router, URL string, jobIDParam string, match func(method string, URL string, req *http.Request) (*Route, error)) (interface{}, error) {
	job, err := findJobByID(ctx, req, routers, URL, jobIDParam)
	if err != nil {
		return nil, err
	}

	route, err := match(job.RequestMethod, job.RequestRouteURI, req)
	if err != nil {
		return nil, err
	}

	switch len(route.Routes) {
	case 0:
		return nil, httputils2.NewHttpMessageError(http.StatusNotFound, fmt.Errorf("not found view with URL %v and method %v", req.URL.Path, req.Method))

	case 1:
		aRoute := route.Routes[0]
		if aRoute.Async == nil {
			return nil, httputils2.NewHttpMessageError(http.StatusInternalServerError, fmt.Errorf("not found async view"))
		}

		aView := aRoute.View

		db, err := aRoute.Async.Connector.DB()
		if err != nil {
			return nil, err
		}

		return async.QueryAll(ctx, db, job, aView.Schema.Slice())
	default:
		return nil, httputils2.NewHttpMessageError(http.StatusInternalServerError, fmt.Errorf("found more than one view with URL %v and method %v", req.URL.Path, req.Method))
	}

}
