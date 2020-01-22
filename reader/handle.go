package reader

import (
	"context"
	"datly/base"
	"encoding/json"
	"github.com/pkg/errors"
	"net/http"
)

//HandleRead handles read request
func HandleRead(srv Service, filters ...base.Filter) base.Handle {
	readerFilters := Filters()
	filters = append(filters, readerFilters.Items...)
	return func(writer http.ResponseWriter, httpRequest *http.Request) {
		err := handleRequest(httpRequest, writer, filters, srv)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		}
	}
}

func handleRequest(httpRequest *http.Request, writer http.ResponseWriter, filters []base.Filter, srv Service) error {
	ctx := context.Background()
	request := &Request{}
	err := request.Init(httpRequest)
	if err != nil {
		return errors.Wrapf(err, "failed to initialise request")
	}

	var toContinue bool
	for i := range filters {
		toContinue, err = filters[i](&request.Request, writer)
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
			base.Registry
			base.StatusInfo
		}{
			Registry:   response.Registry,
			StatusInfo: response.StatusInfo,
		}
		return json.NewEncoder(writer).Encode(output)
	}
	return json.NewEncoder(writer).Encode(response)
}
