package gateway

import (
	"context"
	"database/sql"
	"github.com/viant/datly/gateway/router"
	"github.com/viant/datly/gateway/router/async"
	"github.com/viant/datly/gateway/router/async/handler"
	cusJson "github.com/viant/datly/gateway/router/marshal/json"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/utils/httputils"
	async2 "github.com/viant/xdatly/handler/async"
	"net/http"
	"sync"
)

func NewJobsRoute(URL string, routers []*router.Router, apiKeys []*router.APIKey, marshaller *cusJson.Marshaller) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			URL:    URL,
			Method: http.MethodGet,
		},
		ApiKeys: apiKeys,
		Handler: func(ctx context.Context, response http.ResponseWriter, req *http.Request) {
			records, err := handleJobsRoute(context.Background(), req, routers, nil)
			if err != nil {
				httputils.WriteError(response, err)
				return
			}

			marshal, _ := marshaller.Marshal(records)
			write(response, 200, marshal)
		},
	}
}

func handleJobsRoute(ctx context.Context, req *http.Request, routers []*router.Router, jobID *string) ([]*async2.Job, error) {
	//allJobs := async.NewJobs()
	//for _, aRouter := range routers {
	//	jobs, err := aRouter.PrepareJobs(ctx, req)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	if len(jobs) == 0 {
	//		continue
	//	}
	//
	//	for db, qualifiers := range jobs {
	//		allJobs.AddJobs(db, qualifiers...)
	//	}
	//}
	//
	//index := allJobs.Index()
	//if jobID != nil {
	//	for _, qualifiers := range index {
	//		for _, qualifier := range qualifiers {
	//			qualifier.JobRef = jobID
	//		}
	//	}
	//}
	//
	//errors := shared.NewErrors(0)
	//records := handler.NewRecords()
	//
	//wg := &sync.WaitGroup{}
	//for db, qualifiers := range index {
	//	wg.Add(1)
	//	go queryJobs(ctx, wg, db, qualifiers, errors, records)
	//}
	//
	//wg.Wait()
	//return records.Result(), errors.Error()

	return nil, nil
}

func queryJobs(ctx context.Context, wg *sync.WaitGroup, db *sql.DB, qualifiers []*async.JobQualifier, errors *shared.Errors, records *handler.Records) {
	defer wg.Done()
	jobs, err := async.QueryJobs(ctx, db, qualifiers...)
	if err != nil {
		errors.Append(err)
	} else {
		records.Add(jobs...)
	}
}
