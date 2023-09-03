package gateway

import (
	"context"
	"fmt"
	"github.com/viant/datly/gateway/router"
	cusJson "github.com/viant/datly/gateway/router/marshal/json"
	httputils2 "github.com/viant/datly/utils/httputils"
	"github.com/viant/toolbox"
	async2 "github.com/viant/xdatly/handler/async"
	"net/http"
)

func NewJobByIDRoute(URL string, jobIDParam string, routers []*router.Router, apiKeys []*router.APIKey, marshaller *cusJson.Marshaller) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			URL:    URL,
			Method: http.MethodGet,
		},
		ApiKeys: apiKeys,
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			record, err := findJobByID(context.Background(), req, routers, URL, jobIDParam)
			if err != nil {
				httputils2.WriteError(response, err)
				return
			}

			marshal, _ := marshaller.Marshal(record)
			write(response, 200, marshal)
		},
	}
}

func findJobByID(ctx context.Context, req *http.Request, routers []*router.Router, URL string, idParam string) (*async2.Job, error) {
	parameters, ok := toolbox.ExtractURIParameters(URL, req.URL.Path)
	var jobID *string
	if ok {
		paramValue, ok := parameters[idParam]
		if ok {
			jobID = &paramValue
		}
	}

	if jobID == nil {
		return nil, httputils2.NewHttpMessageError(400, fmt.Errorf("parameter %v is required", idParam))
	}

	jobs, err := handleJobsRoute(ctx, req, routers, jobID)
	if err != nil {
		return nil, err
	}

	switch len(jobs) {
	case 0:
		return nil, httputils2.NewHttpMessageError(http.StatusNotFound, fmt.Errorf("not found job with ID %v for given subject", *jobID))
	case 1:
		return jobs[0], nil
	default:
		return nil, httputils2.NewHttpMessageError(http.StatusInternalServerError, fmt.Errorf("for ID %v found more than one job", *jobID))
	}
}
