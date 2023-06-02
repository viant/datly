package gateway

import (
	"context"
	"database/sql"
	"github.com/viant/datly/httputils"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/async"
	cusJson "github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/shared"
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
		Handler: func(response http.ResponseWriter, req *http.Request, _ *async.Record) {
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

func handleJobsRoute(ctx context.Context, req *http.Request, routers []*router.Router, jobID *string) ([]*async.Record, error) {
	allJobs := async.NewJobs()
	for _, aRouter := range routers {
		jobs, err := aRouter.PrepareJobs(ctx, req)
		if err != nil {
			return nil, err
		}

		if len(jobs) == 0 {
			continue
		}

		for db, qualifiers := range jobs {
			allJobs.AddJobs(db, qualifiers...)
		}
	}

	index := allJobs.Index()
	if jobID != nil {
		for _, qualifiers := range index {
			for _, qualifier := range qualifiers {
				qualifier.JobID = jobID
			}
		}
	}

	errors := shared.NewErrors(0)
	records := async.NewRecords()

	wg := &sync.WaitGroup{}
	for db, qualifiers := range index {
		wg.Add(1)
		go queryJobs(ctx, wg, db, qualifiers, errors, records)
	}

	wg.Wait()
	return records.Result(), errors.Error()
}

func queryJobs(ctx context.Context, wg *sync.WaitGroup, db *sql.DB, qualifiers []*async.JobQualifier, errors *shared.Errors, records *async.Records) {
	defer wg.Done()
	jobs, err := async.QueryJobs(ctx, db, qualifiers...)
	if err != nil {
		errors.Append(err)
	} else {
		records.Add(jobs...)
	}
}
