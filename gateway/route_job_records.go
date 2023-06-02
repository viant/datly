package gateway

import (
	"context"
	"fmt"
	"github.com/viant/datly/httputils"
	"github.com/viant/datly/router"
	"github.com/viant/datly/router/async"
	"github.com/viant/datly/router/marshal/json"
	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/option"
	"net/http"
	"reflect"
	"unsafe"
)

func NewJobRecords(URL string, jobIDParam string, routers []*router.Router, apiKeys []*router.APIKey, marshaller *json.Marshaller, match func(method, URL string, req *http.Request) (*Route, error)) *Route {
	return &Route{
		RouteMeta: RouteMeta{
			URL:    URL,
			Method: http.MethodGet,
		},
		ApiKeys: apiKeys,
		Handler: func(response http.ResponseWriter, req *http.Request, record *async.Record) {
			ctx := context.Background()
			records, err := findAllJobRecords(ctx, req, routers, URL, jobIDParam, match)
			if err != nil {
				httputils.WriteError(response, err)
				return
			}

			marshal, err := marshaller.Marshal(records)
			if err != nil {
				httputils.WriteError(response, httputils.NewHttpStatusError(http.StatusInternalServerError))
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
		return nil, httputils.NewHttpMessageError(http.StatusNotFound, fmt.Errorf("not found view with URL %v and method %v", req.URL.Path, req.Method))

	case 1:
		aRoute := route.Routes[0]
		if aRoute.Async == nil {
			return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, fmt.Errorf("not found async view"))
		}

		aView := aRoute.View

		db, err := aRoute.Async.Connector.DB()
		if err != nil {
			return nil, err
		}

		sliceType := aView.Schema.Slice()
		slice := reflect.New(sliceType.Type)
		appender := sliceType.Appender(unsafe.Pointer(slice.Pointer()))

		reader, err := read.New(ctx, db, "SELECT * FROM "+job.DestinationTable, func() interface{} {
			return appender.Add()
		}, io.Resolve(io.NewResolver().Resolve), option.Tag(view.AsyncTagName))

		if err != nil {
			return nil, err
		}

		if err = reader.QueryAll(ctx, func(row interface{}) error {
			return nil
		}); err != nil {
			return nil, err
		}

		return slice.Elem().Interface(), nil

	default:
		return nil, httputils.NewHttpMessageError(http.StatusInternalServerError, fmt.Errorf("found more than one view with URL %v and method %v", req.URL.Path, req.Method))
	}

}
