package reader

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/viant/datly/base/contract"
	"github.com/viant/datly/filter"
	"github.com/viant/datly/shared"
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
		if !toContinue {
			break
		}
		if err != nil {
			return err
		}
	}
	response := srv.Read(ctx, request)
	response.WriteHeaders(writer)
	if request.DataOnly {
		output := struct {
			Data map[string]interface{}
			contract.StatusInfo
		}{
			Data:       response.Data,
			StatusInfo: response.StatusInfo,
		}
		return json.NewEncoder(writer).Encode(output)
	}
	return json.NewEncoder(writer).Encode(response)
}
