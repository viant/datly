package component

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"

	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/executor/uow"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/handler/logger"
	"github.com/viant/xdatly/handler/response"
	hstate "github.com/viant/xdatly/handler/state"
	"github.com/viant/xunsafe"
)

type componentLocator struct {
	custom    []interface{}
	dispatch  contract.Dispatcher
	constants map[string]interface{}
	path      map[string]string
	form      *hstate.Form
	query     url.Values
	header    http.Header
	logger    logger.Logger

	getRequest func() (*http.Request, error)
}

func (l *componentLocator) Names() []string {
	return nil
}

func (l *componentLocator) Value(ctx context.Context, _ reflect.Type, name string) (interface{}, bool, error) {
	order := uow.BindingOrder(ctx)
	if _, _, scoped := uow.FromContext(ctx); scoped && order == "" {
		return nil, false, fmt.Errorf("component binding %q has no reserved declaration slot", name)
	}
	ctx = uow.PrepareChild(ctx, uow.RelationBinding, order)
	method, URI := shared.ExtractPath(name)
	request, err := l.getRequest()
	if err != nil {
		return nil, false, err
	}
	request = sanitizeSelectorRequest(request)
	form := l.form
	value, err := l.dispatch.Dispatch(ctx, &contract.Path{Method: method, URI: URI}, contract.WithRequest(request),
		contract.WithConstants(l.constants),
		contract.WithPath(l.path),
		contract.WithQuery(sanitizeSelectorQuery(l.query)),
		contract.WithForm(form),
		contract.WithLogger(l.logger),
		contract.WithHeader(l.header),
	)
	err = updateErrWithResponseStatus(err, value)
	return value, err == nil, err
}

func sanitizeSelectorQuery(query url.Values) url.Values {
	sanitized, _ := sanitizeSelectorQueryWithRemoval(query)
	return sanitized
}

func sanitizeSelectorQueryWithRemoval(query url.Values) (url.Values, bool) {
	if len(query) == 0 {
		return query, false
	}
	removed := false
	result := make(url.Values, len(query))
	for key, values := range query {
		if isSelectorQueryKey(key) {
			removed = true
			continue
		}
		result[key] = append([]string(nil), values...)
	}
	if !removed {
		return query, false
	}
	return result, true
}

func sanitizeSelectorRequest(request *http.Request) *http.Request {
	if request == nil || request.URL == nil || request.URL.RawQuery == "" {
		return request
	}
	sanitized, removed := sanitizeSelectorQueryWithRemoval(request.URL.Query())
	if !removed {
		return request
	}
	cloned := request.Clone(request.Context())
	cloned.URL = cloneURL(request.URL)
	cloned.URL.RawQuery = sanitized.Encode()
	return cloned
}

func cloneURL(src *url.URL) *url.URL {
	if src == nil {
		return nil
	}
	cloned := *src
	return &cloned
}

func isSelectorQueryKey(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case view.FieldsQuery, view.OrderByQuery, view.LimitQuery, view.OffsetQuery, view.PageQuery, view.CriteriaQuery:
		return true
	default:
		return false
	}
}

func updateErrWithResponseStatus(err error, response interface{}) error {
	var statusErr error
	responseStatus, ok := tryExtractResponseStatus(response)
	if ok && responseStatus.Status == "error" {
		statusErr = errors.New(responseStatus.Message)
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

// newComponentLocator returns component locator
func newComponentLocator(opts ...locator.Option) (kind.Locator, error) {
	options := locator.NewOptions(opts)
	if options.Dispatcher == nil {
		return nil, fmt.Errorf("dispatcher was empty")
	}
	ret := &componentLocator{
		custom:     options.Custom,
		dispatch:   options.Dispatcher,
		constants:  options.Constants,
		getRequest: options.GetRequest,
		logger:     options.Logger,
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
