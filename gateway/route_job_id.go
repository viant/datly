package gateway

import (
	"context"
	"fmt"
	"github.com/viant/datly/httputils"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/async"
	cusJson "github.com/viant/datly/router/marshal/json"
	"github.com/viant/toolbox"
	"net/http"
)

func NewJobByIDRoute(URL string, jobIDParam string, routers []*router.Router, apiKeys []*router.APIKey, marshaller *cusJson.Marshaller) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			URL:    URL,
			Method: http.MethodGet,
		},
		ApiKeys: apiKeys,
		Handler: func(response http.ResponseWriter, req *http.Request, _ *async.Record) {
			record, err := findJobByID(context.Background(), req, routers, URL, jobIDParam)
			if err != nil {
				httputils.WriteError(response, err)
				return
			}

			marshal, _ := marshaller.Marshal(record)
			write(response, 200, marshal)
		},
	}
}

func findJobByID(ctx context.Context, req *http.Request, routers []*router.Router, URL string, idParam string) (*async.Record, error) {
	parameters, ok := toolbox.ExtractURIParameters(URL, req.URL.Path)
	var jobID *string
	if ok {
		paramValue, ok := parameters[idParam]
		if ok {
			jobID = &paramValue
		}
	}

	if jobID == nil {
		return nil, httputils.NewHttpMessageError(400, fmt.Errorf("parameter %v is required", idParam))
	}

	jobs, err := handleJobsRoute(ctx, req, routers, jobID)
	if err != nil {
		return nil, err
	}

	switch len(jobs) {
	case 0:
		return nil, httputils.NewHttpMessageError(http.StatusNotFound, fmt.Errorf("not found job with ID %v for given subject", *jobID))
	case 1:
		return jobs[0], nil
	default:
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, fmt.Errorf("for ID %v found more than one job", *jobID))
	}
}
