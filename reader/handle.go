package reader

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/datly/filter"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox"
	"net/http"
)

//HandleRead handles read request
func HandleRead(srv Service, filters ...filter.Filter) shared.Handle {

	readerFilters := Filters()
	filters = append(filters, readerFilters.Items...)
	return func(writer http.ResponseWriter, httpRequest *http.Request) {
		err := handleRequest(httpRequest, writer, filters, srv)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
	}
}

func handleRequest(httpRequest *http.Request, writer http.ResponseWriter, filters []filter.Filter, srv Service) error {
	ctx := context.Background()
	request := &Request{}
	err := request.Init(httpRequest)
	if err != nil {
		return errors.Wrapf(err, "failed to initialise request")
	}

	var toContinue bool
	for i := range filters {
		toContinue, err = filters[i](ctx, &request.Request, writer)
		if err != nil {
			if err == shared.FilterAbortRequestError {
				return nil
			}
			return err
		}
		if !toContinue {
			break
		}
	}
	if shared.IsLoggingEnabled() {
		toolbox.Dump(request)
	}
	response := srv.Read(ctx, request)
	response.WriteHeaders(writer)
	metrics := request.QueryParams.Get(shared.Metrics)
	info := response.ApplyFilter(metrics)
	toolbox.Dump(info)
	return json.NewEncoder(writer).Encode(response)
}
