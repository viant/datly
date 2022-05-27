package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/criteria"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"unsafe"
)

type RequestMetadata struct {
	URI      string
	Index    Index
	MainView *view.View
}

func CreateSelectorsFromRoute(ctx context.Context, route *Route, request *http.Request, views ...*ViewDetails) (view.Selectors, error) {
	requestMetadata := NewRequestMetadata(route)
	requestParams, err := NewRequestParameters(request, route)
	if err != nil {
		return nil, err
	}

	return CreateSelectors(ctx, route._caser, requestMetadata, requestParams, views...)
}

func NewRequestMetadata(route *Route) *RequestMetadata {
	requestMetadata := &RequestMetadata{
		URI:      route.URI,
		Index:    route.Index,
		MainView: route.View,
	}
	return requestMetadata
}

func CreateSelectors(ctx context.Context, inputFormat format.Case, requestMetadata *RequestMetadata, requestParams *RequestParams, views ...*ViewDetails) (view.Selectors, error) {
	selectors := view.Selectors{}

	if err := buildSelectors(ctx, inputFormat, requestMetadata, &selectors, views, requestParams); err != nil {
		return nil, err
	}

	return selectors, nil
}

func buildSelectors(ctx context.Context, inputFormat format.Case, requestMetadata *RequestMetadata, selectors *view.Selectors, viewsDetails []*ViewDetails, requestParams *RequestParams) error {
	wg := sync.WaitGroup{}
	errors := shared.NewErrors(0)
	for _, details := range viewsDetails {
		selector := selectors.Lookup(details.View)
		wg.Add(1)
		go func(details *ViewDetails, requestMetadata *RequestMetadata, requestParams *RequestParams, selector *view.Selector) {
			defer wg.Done()
			if err := populateSelector(selector, inputFormat, details, requestParams); err != nil {
				errors.Append(err)
				return
			}

			if details.View.Template == nil || len(details.View.Template.Parameters) == 0 {
				return
			}

			selector.Parameters.Init(details.View)
			params := &selector.Parameters
			if err := buildSelectorParameters(ctx, inputFormat, details, xunsafe.AsPointer(params.Values), xunsafe.AsPointer(params.Has), details.View.Template.Parameters, requestParams, requestMetadata); err != nil {
				errors.Append(err)
				return
			}
		}(details, requestMetadata, requestParams, selector)
	}

	wg.Wait()
	return errors.Error()
}

func populateSelector(selector *view.Selector, inputFormat format.Case, details *ViewDetails, params *RequestParams) error {
	for _, ns := range details.Prefixes {
		if fields := params.queryParam(ns+string(Fields), ""); fields != "" {
			if err := buildFields(inputFormat, details.View, selector, fields); err != nil {
				return err
			}
		}

		if offset := params.queryParam(ns+string(Offset), ""); offset != "" {
			if err := buildOffset(details.View, selector, offset); err != nil {
				return err
			}
		}

		if offset := params.queryParam(ns+string(Limit), ""); offset != "" {
			if err := buildLimit(details.View, selector, offset); err != nil {
				return err
			}
		}

		if orderBy := params.queryParam(ns+string(OrderBy), ""); orderBy != "" {
			if err := buildOrderBy(details.View, selector, orderBy); err != nil {
				return err
			}
		}

		if criteria := params.queryParam(ns+string(Criteria), ""); criteria != "" {
			if err := buildCriteria(details.View, selector, criteria); err != nil {
				return err
			}
		}
	}

	return nil
}

func buildSelectorParameters(ctx context.Context, inputFormat format.Case, parent *ViewDetails, paramsPtr, presencePtr unsafe.Pointer, parameters []*view.Parameter, requestParams *RequestParams, requestMetadata *RequestMetadata) error {
	var err error
	for _, parameter := range parameters {
		switch parameter.In.Kind {
		case view.QueryKind:
			if err = addQueryParam(ctx, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case view.PathKind:
			if err = addPathParam(ctx, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case view.HeaderKind:
			if err = addHeaderParam(ctx, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case view.CookieKind:
			if err = addCookieParam(ctx, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case view.DataViewKind:
			if err = addViewParam(ctx, inputFormat, parent, paramsPtr, presencePtr, parameter, requestParams, requestMetadata); err != nil {
				return err
			}

		case view.RequestBodyKind:
			if err = addRequestBodyParam(paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}
		}
	}
	return nil
}

func addRequestBodyParam(paramsPtr unsafe.Pointer, presencePtr unsafe.Pointer, param *view.Parameter, requestParams *RequestParams) error {
	if param.Required != nil && *param.Required && requestParams.requestBody == nil {
		return fmt.Errorf("parameter %v is required", param.Name)
	}

	if requestParams.requestBody == nil {
		return nil
	}

	if err := param.Set(paramsPtr, requestParams.requestBody); err != nil {
		return err
	}

	param.UpdatePresence(presencePtr)
	return nil
}

func addCookieParam(ctx context.Context, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *view.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, ptr, presencePtr, parameter, params.cookie(parameter.In.Name))
}

func addHeaderParam(ctx context.Context, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *view.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, ptr, presencePtr, parameter, params.header(parameter.In.Name))
}

func addQueryParam(ctx context.Context, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *view.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, ptr, presencePtr, parameter, params.queryParam(parameter.In.Name, ""))
}

func addPathParam(ctx context.Context, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *view.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, ptr, presencePtr, parameter, params.pathVariable(parameter.In.Name, ""))
}

func addViewParam(ctx context.Context, inputFormat format.Case, viewDetails *ViewDetails, paramsPtr, presencePtr unsafe.Pointer, param *view.Parameter, params *RequestParams, requestMetadata *RequestMetadata) error {
	aView := param.View()
	destSlice := reflect.New(aView.Schema.SliceType()).Interface()
	session := reader.NewSession(destSlice, aView)
	session.Parent = viewDetails.View
	newIndex := Index{}
	if err := newIndex.Init(aView, ""); err != nil {
		return err
	}

	newRequestMetadata := &RequestMetadata{
		URI:      requestMetadata.URI,
		Index:    newIndex,
		MainView: nil,
	}

	selectors, err := CreateSelectors(ctx, inputFormat, newRequestMetadata, params, &ViewDetails{View: aView})
	if err != nil {
		return err
	}

	session.Selectors = selectors
	if err = reader.New().Read(ctx, session); err != nil {
		return err
	}
	ptr := xunsafe.AsPointer(destSlice)
	paramLen := aView.Schema.Slice().Len(ptr)
	switch paramLen {
	case 0:
		if param.Required != nil && *param.Required {
			return fmt.Errorf("parameter %v value is required but no view was found", param.Name)
		}
	case 1:
		holder := aView.Schema.Slice().ValuePointerAt(ptr, 0)
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

func convertAndSet(ctx context.Context, paramPtr, presencePtr unsafe.Pointer, parameter *view.Parameter, rawValue string) error {
	if parameter.IsRequired() && rawValue == "" {
		return fmt.Errorf("parameter %v is required", parameter.Name)
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

func buildFields(inputFormat format.Case, aView *view.View, selector *view.Selector, fieldsQuery string) error {
	fieldIt := NewParamIt(fieldsQuery)
	for fieldIt.Has() {
		param, err := fieldIt.Next()
		if err != nil {
			return err
		}

		columnName := param.Value
		if !aView.IsHolder(columnName) {
			columnName = inputFormat.Format(param.Value, aView.Caser)
		}

		if err = canUseColumn(aView, columnName); err != nil {
			return err
		}

		selector.Columns = append(selector.Columns, columnName)
		if !aView.IsHolder(param.Value) {
			selector.Fields = append(selector.Fields, inputFormat.Format(param.Value, format.CaseUpperCamel))
		} else {
			selector.Fields = append(selector.Fields, param.Value)
		}
	}

	return nil
}

func canUseColumn(aView *view.View, columnName string) error {
	_, ok := aView.ColumnByName(columnName)
	if !ok {
		return fmt.Errorf("not found column %v in view %v", columnName, aView.Name)
	}
	return nil
}

func buildOffset(aView *view.View, selector *view.Selector, offsetQuery string) error {
	fieldIt := NewParamIt(offsetQuery)
	for fieldIt.Has() {
		param, err := fieldIt.Next()
		if err != nil {
			return err
		}
		if !aView.CanUseSelectorOffset() {
			return fmt.Errorf("can't use selector offset on %v view", aView.Name)
		}

		if err = updateSelectorOffset(selector, param.Value); err != nil {
			return err
		}
	}

	return nil
}

func updateSelectorOffset(selector *view.Selector, offset string) error {
	offsetConv, err := strconv.Atoi(offset)
	if err != nil {
		return err
	}

	selector.Offset = offsetConv
	return nil
}

func buildLimit(aView *view.View, selector *view.Selector, limitQuery string) error {
	fieldIt := NewParamIt(limitQuery)
	for fieldIt.Has() {
		param, err := fieldIt.Next()
		if err != nil {
			return err
		}
		if !aView.CanUseSelectorLimit() {
			return fmt.Errorf("can't use selector limit on %v view", aView.Name)
		}

		if err = updateSelectorLimit(selector, param.Value); err != nil {
			return err
		}

	}

	return nil
}

func updateSelectorLimit(selector *view.Selector, limit string) error {
	limitConv, err := strconv.Atoi(limit)
	if err != nil {
		return err
	}

	selector.Limit = limitConv
	return nil
}

func buildOrderBy(aView *view.View, selector *view.Selector, orderByQuery string) error {
	fieldIt := NewParamIt(orderByQuery)
	for fieldIt.Has() {
		param, err := fieldIt.Next()
		if err != nil {
			return err
		}
		if err = canUseOrderBy(aView, param.Value); err != nil {
			return err
		}

		selector.OrderBy = param.Value
	}
	return nil
}

func canUseOrderBy(view *view.View, orderBy string) error {
	if !view.CanUseSelectorOrderBy() {
		return fmt.Errorf("can't use orderBy %v on view %v", orderBy, view.Name)
	}

	_, ok := view.ColumnByName(orderBy)
	if !ok {
		return fmt.Errorf("not found column %v on view %v", orderBy, view.Name)
	}

	return nil
}

func buildCriteria(aView *view.View, selector *view.Selector, criteriaQuery string) error {
	fieldIt := NewParamIt(criteriaQuery)
	for fieldIt.Has() {

		param, err := fieldIt.Next()
		if err != nil {
			return err
		}

		if !aView.CanUseSelectorCriteria() {
			return fmt.Errorf("can't use criteria on view %v", aView.Name)
		}

		sanitized, err := criteria.Parse(param.Value, aView.IndexedColumns())
		if err != nil {
			return err
		}

		selector.Criteria = sanitized.Expression
		selector.Placeholders = sanitized.Placeholders
	}

	return nil
}
