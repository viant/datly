package component

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/handler/response"
	hstate "github.com/viant/xdatly/handler/state"
	"github.com/viant/xunsafe"
	"net/http"
	"net/url"
	"reflect"
)

type componentLocator struct {
	custom     []interface{}
	dispatch   contract.Dispatcher
	constants  map[string]interface{}
	path       map[string]string
	form       *hstate.Form
	query      url.Values
	header     http.Header
	getRequest func() (*http.Request, error)
}

func (l *componentLocator) Names() []string {
	return nil
}

func (l *componentLocator) Value(ctx context.Context, name string) (interface{}, bool, error) {
	method, URI := shared.ExtractPath(name)
	request, err := l.getRequest()
	if err != nil {
		return nil, false, err
	}
	form := l.form
	value, err := l.dispatch.Dispatch(ctx, &contract.Path{Method: method, URI: URI}, contract.WithRequest(request),
		contract.WithConstants(l.constants),
		contract.WithPath(l.path),
		contract.WithQuery(l.query),
		contract.WithForm(form),
		contract.WithHeader(l.header),
	)
	err = updateErrWithResponseStatus(err, value)
	return value, err == nil, err
}

func updateErrWithResponseStatus(err error, response interface{}) error {
	var statusErr error
	responseStatus, ok := tryExtractResponseStatus(response)
	if ok && responseStatus.Status == "error" {
		statusErr = fmt.Errorf(responseStatus.Message)
	}

	if statusErr != nil {
		if err == nil {
			err = statusErr
		} else {
			err = fmt.Errorf("two errors: %w, %w", err, statusErr)
		}
	}
	return err
}

func tryExtractResponseStatus(value interface{}) (*response.Status, bool) {
	rType := reflect.TypeOf(value)
	if rType == nil {
		return nil, false
	}
	xStruct := xunsafe.NewStruct(rType)
	xField := xStruct.MatchByType(reflect.TypeOf(response.Status{}))
	if xField == nil {
		return nil, false
	}
	ptr := xunsafe.AsPointer(value)
	uPtr := xField.ValuePointer(ptr)
	if uPtr == nil {
		return nil, false
	}
	return (*response.Status)(uPtr), true
}

// TODO passed locator options to dispatcher so that this wil not be nil
var dispatcher contract.Dispatcher

// newComponentLocator returns component locator
func newComponentLocator(opts ...locator.Option) (kind.Locator, error) {
	options := locator.NewOptions(opts)
	if options.Dispatcher == nil {
		options.Dispatcher = dispatcher
	}
	if options.Dispatcher == nil {
		return nil, fmt.Errorf("dispatcher was empty")
	}
	dispatcher = options.Dispatcher
	ret := &componentLocator{
		custom:     options.Custom,
		dispatch:   options.Dispatcher,
		constants:  options.Constants,
		getRequest: options.GetRequest,
		form:       options.Form,
		query:      options.Query,
		header:     options.Header,
		path:       options.Path,
	}
	return ret, nil
}

func init() {
	locator.Register(state.KindComponent, newComponentLocator)
}
