package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/data"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/sanitize"
	"github.com/viant/datly/shared"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

type RequestMetadata struct {
	URI      string
	Index    Index
	MainView *data.View
}

func CreateSelectorsFromRoute(ctx context.Context, route *Route, request *http.Request, views ...*data.View) (data.Selectors, error) {
	requestMetadata := &RequestMetadata{
		URI:      route.URI,
		Index:    route.Index,
		MainView: route.View,
	}

	requestParams := NewRequestParameters(request, route.URI)
	return CreateSelectors(ctx, requestMetadata, requestParams, views...)
}

func CreateSelectors(ctx context.Context, requestMetadata *RequestMetadata, requestParams *RequestParams, views ...*data.View) (data.Selectors, error) {
	selectors := data.Selectors{}

	if err := buildParameters(ctx, requestMetadata, &selectors, views, requestParams); err != nil {
		return nil, err
	}

	for paramName, paramValue := range requestParams.queryIndex {
		paramName = strings.ToLower(paramName)

		switch paramName {
		case string(Fields):
			if err := buildFields(&selectors, requestMetadata, paramValue[0]); err != nil {
				return nil, err
			}

		case string(Offset):
			if err := buildOffset(&selectors, requestMetadata, paramValue[0]); err != nil {
				return nil, err
			}

		case string(Limit):
			if err := buildLimit(&selectors, requestMetadata, paramValue[0]); err != nil {
				return nil, err
			}

		case string(OrderBy):
			if err := buildOrderBy(&selectors, requestMetadata, paramValue[0]); err != nil {
				return nil, err
			}

		case string(Criteria):
			if err := buildCriteria(&selectors, requestMetadata, paramValue[0]); err != nil {
				return nil, err
			}
		}
	}

	return selectors, nil
}

func buildParameters(ctx context.Context, requestMetadata *RequestMetadata, selectors *data.Selectors, views []*data.View, requestParams *RequestParams) error {
	wg := sync.WaitGroup{}
	errors := shared.NewErrors(0)
	for _, view := range views {
		if view.Template == nil || len(view.Template.Parameters) == 0 {
			continue
		}

		wg.Add(1)
		go func(view *data.View) {
			defer wg.Done()
			selector := selectors.Lookup(view)
			selector.Parameters.Init(view)
			params := &selector.Parameters
			if err := buildSelectorParameters(ctx, view, xunsafe.AsPointer(params.Values), xunsafe.AsPointer(params.Has), view.Template.Parameters, requestParams, requestMetadata); err != nil {
				errors.Append(err)
			}
		}(view)
	}

	wg.Wait()
	return errors.Error()
}

func buildSelectorParameters(ctx context.Context, parent *data.View, paramsPtr, presencePtr unsafe.Pointer, parameters []*data.Parameter, requestParams *RequestParams, requestMetadata *RequestMetadata) error {
	var err error
	for _, parameter := range parameters {
		switch parameter.In.Kind {
		case data.QueryKind:
			if err = addQueryParam(ctx, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case data.PathKind:
			if err = addPathParam(ctx, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case data.HeaderKind:
			if err = addHeaderParam(ctx, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case data.CookieKind:
			if err = addCookieParam(ctx, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case data.DataViewKind:
			if err = addViewParam(ctx, parent, paramsPtr, presencePtr, parameter, requestParams, requestMetadata); err != nil {
				return err
			}
		}
	}
	return nil
}

func addCookieParam(ctx context.Context, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *data.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, ptr, presencePtr, parameter, params.cookie(parameter.In.Name))
}

func addHeaderParam(ctx context.Context, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *data.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, ptr, presencePtr, parameter, params.header(parameter.In.Name))
}

func addQueryParam(ctx context.Context, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *data.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, ptr, presencePtr, parameter, params.queryParam(parameter.In.Name, ""))
}

func addPathParam(ctx context.Context, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *data.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, ptr, presencePtr, parameter, params.pathVariable(parameter.In.Name, ""))
}

func addViewParam(ctx context.Context, parent *data.View, paramsPtr, presencePtr unsafe.Pointer, param *data.Parameter, params *RequestParams, requestMetadata *RequestMetadata) error {
	view := param.View()
	destSlice := reflect.New(view.Schema.SliceType()).Interface()
	session := reader.NewSession(destSlice, view)
	session.Parent = parent
	newIndex := Index{}
	if err := newIndex.Init(view); err != nil {
		return err
	}

	newRequestMetadata := &RequestMetadata{
		URI:      requestMetadata.URI,
		Index:    newIndex,
		MainView: nil,
	}

	selectors, err := CreateSelectors(ctx, newRequestMetadata, params, view)
	if err != nil {
		return err
	}

	session.Selectors = selectors
	if err = reader.New().Read(ctx, session); err != nil {
		return err
	}
	ptr := xunsafe.AsPointer(destSlice)
	paramLen := view.Schema.Slice().Len(ptr)
	switch paramLen {
	case 0:
		if param.Required != nil && *param.Required {
			return fmt.Errorf("parameter %v value is required but no data was found", param.Name)
		}
	case 1:
		holder := view.Schema.Slice().ValuePointerAt(ptr, 0)
		if err = param.Set(paramsPtr, holder); err != nil {
			return err
		}

		param.UpdatePresence(presencePtr)
		return nil

	default:
		return fmt.Errorf("parameter %v return more than one value", param.Name)
	}

	return nil
}

func convertAndSet(ctx context.Context, paramPtr, presencePtr unsafe.Pointer, parameter *data.Parameter, rawValue string) error {
	if parameter.IsRequired() && rawValue == "" {
		return fmt.Errorf("query parameter %v is required", parameter.Name)
	}

	if rawValue == "" {
		return nil
	}

	if err := parameter.ConvertAndSet(ctx, paramPtr, rawValue); err != nil {
		return err
	}

	parameter.UpdatePresence(presencePtr)
	return nil
}

func buildFields(selectors *data.Selectors, requestMetadata *RequestMetadata, fieldsQuery string) error {
	for _, field := range strings.Split(fieldsQuery, "|") {
		viewField := strings.Split(field, ".")

		switch len(viewField) {
		case 1:
			if requestMetadata.MainView == nil {
				continue
			}

			if err := canUseColumn(requestMetadata.MainView, viewField[0]); err != nil {
				return err
			}

			selector := selectors.Lookup(requestMetadata.MainView)
			selector.Columns = append(selector.Columns, field)

		case 2:
			view, err := viewByPrefix(viewField[0], requestMetadata)
			if err != nil {
				continue
			}

			if err = canUseColumn(view, viewField[1]); err != nil {
				return err
			}

			selector := selectors.Lookup(view)
			selector.Columns = append(selector.Columns, viewField[1])

		default:
			return NewUnsupportedFormat(string(Fields), field)
		}
	}

	return nil
}

func viewByPrefix(prefix string, requestMetadata *RequestMetadata) (*data.View, error) {
	return requestMetadata.Index.ViewByPrefix(prefix)
}

func canUseColumn(view *data.View, columnName string) error {
	column, ok := view.ColumnByName(columnName)
	if !ok {
		return fmt.Errorf("not found column %v in view %v", columnName, view.Name)
	}

	if !column.Filterable {
		return fmt.Errorf("column %v is not filterable", columnName)
	}

	return nil
}

func buildOffset(selectors *data.Selectors, requestMetadata *RequestMetadata, offsetQuery string) error {
	for _, offset := range strings.Split(offsetQuery, "|") {
		viewOffset := strings.Split(offset, ".")
		switch len(viewOffset) {
		case 1:
			if requestMetadata.MainView == nil {
				continue
			}

			if !requestMetadata.MainView.CanUseSelectorOffset() {
				return fmt.Errorf("can't use selector offset on %v view", requestMetadata.MainView.Name)
			}

			if err := updateSelectorOffset(selectors, viewOffset[1], requestMetadata.MainView); err != nil {
				return err
			}

		case 2:
			view, err := viewByPrefix(viewOffset[0], requestMetadata)
			if err != nil {
				continue
			}

			if !view.CanUseSelectorOffset() {
				return fmt.Errorf("can't use selector offset on %v view", view.Name)
			}

			if err = updateSelectorOffset(selectors, viewOffset[1], view); err != nil {
				return err
			}

		default:
			return NewUnsupportedFormat(string(Offset), offset)
		}
	}

	return nil
}

func updateSelectorOffset(selectors *data.Selectors, offset string, view *data.View) error {
	offsetConv, err := strconv.Atoi(offset)
	if err != nil {
		return err
	}

	selector := selectors.Lookup(view)
	selector.Offset = offsetConv
	return nil
}

func buildLimit(selectors *data.Selectors, requestMetadata *RequestMetadata, limitQuery string) error {
	for _, limit := range strings.Split(limitQuery, "|") {
		viewLimit := strings.Split(limit, ".")
		switch len(viewLimit) {
		case 1:
			if requestMetadata.MainView == nil {
				continue
			}

			if !requestMetadata.MainView.CanUseSelectorLimit() {
				return fmt.Errorf("can't use selector limit on %v view", requestMetadata.MainView.Name)
			}

			if err := updateSelectorLimit(selectors, viewLimit[0], requestMetadata.MainView); err != nil {
				return err
			}

		case 2:
			view, err := viewByPrefix(viewLimit[0], requestMetadata)
			if err != nil {
				continue
			}

			if !view.CanUseSelectorLimit() {
				return fmt.Errorf("can't use selector limit on %v view", view.Name)
			}

			if err = updateSelectorLimit(selectors, viewLimit[1], view); err != nil {
				return err
			}

		default:
			return NewUnsupportedFormat(string(Limit), limit)
		}
	}

	return nil
}

func updateSelectorLimit(selectors *data.Selectors, limit string, view *data.View) error {
	limitConv, err := strconv.Atoi(limit)
	if err != nil {
		return err
	}

	selector := selectors.Lookup(view)
	selector.Limit = limitConv
	return nil
}

func buildOrderBy(selectors *data.Selectors, requestMetadata *RequestMetadata, orderByQuery string) error {
	for _, orderBy := range strings.Split(orderByQuery, "|") {
		viewOrderBy := strings.Split(orderBy, ".")
		switch len(viewOrderBy) {
		case 1:
			if requestMetadata.MainView == nil {
				continue
			}

			if err := canUseOrderBy(requestMetadata.MainView, viewOrderBy[0]); err != nil {
				return err
			}

			selector := selectors.Lookup(requestMetadata.MainView)
			selector.OrderBy = viewOrderBy[0]

		case 2:
			view, err := viewByPrefix(viewOrderBy[0], requestMetadata)
			if err != nil {
				continue
			}

			if err = canUseOrderBy(view, viewOrderBy[1]); err != nil {
				return err
			}

			selector := selectors.Lookup(view)
			selector.OrderBy = viewOrderBy[1]

		default:
			return NewUnsupportedFormat(string(OrderBy), orderBy)
		}
	}
	return nil
}

func canUseOrderBy(view *data.View, orderBy string) error {
	if !view.CanUseSelectorOrderBy() {
		return fmt.Errorf("can't use orderBy %v on view %v", orderBy, view.Name)
	}

	_, ok := view.ColumnByName(orderBy)
	if !ok {
		return fmt.Errorf("not found column %v on view %v", orderBy, view.Name)
	}

	return nil
}

func buildCriteria(selectors *data.Selectors, requestMetadata *RequestMetadata, criteriaQuery string) error {
	for _, criteria := range strings.Split(criteriaQuery, "|") {
		viewCriteria := strings.Split(criteria, ".")

		switch len(viewCriteria) {
		case 1:
			if requestMetadata.MainView == nil {
				continue
			}

			if err := addSelectorCriteria(selectors, requestMetadata.MainView, viewCriteria[0]); err != nil {
				return err
			}

		case 2:
			view, err := viewByPrefix(viewCriteria[0], requestMetadata)
			if err != nil {
				continue
			}

			if err = addSelectorCriteria(selectors, view, viewCriteria[1]); err != nil {
				return err
			}

		default:
			return NewUnsupportedFormat(string(Criteria), criteria)
		}
	}

	return nil
}

func addSelectorCriteria(selectors *data.Selectors, view *data.View, criteria string) error {
	if !view.CanUseSelectorCriteria() {
		return fmt.Errorf("can't use criteria on view %v", view.Name)
	}

	criteriaSanitized, err := sanitizeCriteria(criteria, view)
	if err != nil {
		return err
	}

	selector := selectors.Lookup(view)
	selector.Criteria = criteriaSanitized
	return nil
}

func sanitizeCriteria(criteria string, view *data.View) (string, error) {
	node, err := sanitize.Parse([]byte(criteria))
	if err != nil {
		return "", err
	}

	sb := strings.Builder{}
	if err = node.Sanitize(&sb, view.IndexedColumns()); err != nil {
		return "", err
	}

	return sb.String(), nil
}

func dereferenceIfNeeded(schema *data.Schema, params reflect.Value) interface{} {
	if schema.Type().Kind() == reflect.Ptr {
		return params.Interface()
	}

	return params.Elem().Interface()
}

func getValue(schema *data.Schema) reflect.Value {
	if schema.Type().Kind() == reflect.Ptr {
		return reflect.New(schema.Type().Elem())
	}

	return reflect.New(schema.Type())
}
