package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/criteria"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"net/http"
	"os"
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
			if err := populateSelector(ctx, selector, inputFormat, details, requestParams, requestMetadata); err != nil {
				errors.Append(err)
				return
			}

			if details.View.Template == nil || len(details.View.Template.Parameters) == 0 {
				return
			}

			selector.Parameters.Init(details.View)
			params := &selector.Parameters
			if err := buildSelectorParameters(ctx, selector, inputFormat, details, xunsafe.AsPointer(params.Values), xunsafe.AsPointer(params.Has), details.View.Template.Parameters, requestParams, requestMetadata); err != nil {
				errors.Append(err)
				return
			}
		}(details, requestMetadata, requestParams, selector)
	}

	wg.Wait()
	return errors.Error()
}

func populateSelector(ctx context.Context, selector *view.Selector, inputFormat format.Case, details *ViewDetails, params *RequestParams, metadata *RequestMetadata) error {

	for i, ns := range details.Prefixes {
		if i == 0 || details.View.Selector.FieldsParam == nil {
			if err := populateFields(ctx, selector, inputFormat, details, params, ns, metadata); err != nil {
				return err
			}
		}

		if i == 0 || details.View.Selector.OffsetParam == nil {
			if err := populateOffset(ctx, selector, inputFormat, details, params, ns, metadata, selector); err != nil {
				return err
			}
		}

		if i == 0 || details.View.Selector.OrderByParam == nil {
			if err := populateOrderBy(ctx, selector, inputFormat, details, params, ns, metadata); err != nil {
				return err
			}
		}

		if i == 0 || details.View.Selector.LimitParam == nil {
			if err := populateLimit(ctx, selector, inputFormat, details, params, ns, metadata); err != nil {
				return err
			}
		}

		if i == 0 || details.View.Selector.CriteriaParam == nil {
			if err := populateCriteria(ctx, selector, inputFormat, details, params, ns, metadata); err != nil {
				return err
			}
		}
	}

	return nil
}

func populateCriteria(ctx context.Context, selector *view.Selector, inputFormat format.Case, details *ViewDetails, params *RequestParams, ns string, metadata *RequestMetadata) error {
	criteriaExpression, err := criteriaValue(ctx, inputFormat, details, params, ns, metadata, selector)
	if err != nil || criteriaExpression == "" {
		return err
	}

	if !details.View.Selector.Constraints.Criteria {
		return fmt.Errorf("can't use criteria on view %v", details.View.Name)
	}

	sanitizedCriteria, err := criteria.Parse(criteriaExpression, details.View.IndexedColumns(), details.View.Selector.Constraints.SqlMethodsIndexed())
	if err != nil {
		return err
	}

	selector.Criteria = sanitizedCriteria.Expression
	selector.Placeholders = sanitizedCriteria.Placeholders
	return nil
}

func criteriaValue(ctx context.Context, inputFormat format.Case, details *ViewDetails, params *RequestParams, ns string, metadata *RequestMetadata, selector *view.Selector) (string, error) {
	param := details.View.Selector.CriteriaParam
	if param == nil {
		return params.queryParam(ns+string(Criteria), ""), nil
	}

	paramValue, err := extractParamValue(ctx, param, details, inputFormat, params, metadata, selector)
	if err != nil || paramValue == nil {
		return "", err
	}

	if actual, ok := paramValue.(string); ok {
		return actual, nil
	}

	return "", typeMismatchError(param, paramValue)
}

func populateLimit(ctx context.Context, selector *view.Selector, inputFormat format.Case, details *ViewDetails, params *RequestParams, ns string, metadata *RequestMetadata) error {
	limit, err := limitValue(ctx, inputFormat, details, params, ns, metadata, selector)
	if err != nil || limit == 0 {
		return err
	}

	if !details.View.Selector.Constraints.Limit {
		return fmt.Errorf("can't use Limit on view %v", details.View.Name)
	}

	if limit <= details.View.Selector.Limit || details.View.Selector.Limit == 0 {
		selector.Limit = limit
	}

	return nil
}

func limitValue(ctx context.Context, inputFormat format.Case, details *ViewDetails, params *RequestParams, ns string, metadata *RequestMetadata, selector *view.Selector) (int, error) {
	param := details.View.Selector.LimitParam
	if param == nil {
		return parseInt(params.queryParam(ns+string(Limit), ""))
	}

	paramValue, err := extractParamValue(ctx, param, details, inputFormat, params, metadata, selector)
	if err != nil {
		return 0, err
	}

	return asInt(paramValue, param)
}

func parseInt(queryParam string) (int, error) {
	if queryParam == "" {
		return 0, nil
	}
	return strconv.Atoi(queryParam)
}

func populateOrderBy(ctx context.Context, selector *view.Selector, inputFormat format.Case, details *ViewDetails, params *RequestParams, ns string, metadata *RequestMetadata) error {
	orderBy, err := orderByValue(ctx, inputFormat, details, params, ns, metadata, selector)
	if err != nil {
		return err
	}

	if orderBy == "" {
		return nil
	}

	if !details.View.Selector.Constraints.OrderBy {
		return fmt.Errorf("can't use offset on view %v", details.View.Name)
	}

	col, ok := details.View.ColumnByName(orderBy)
	if !ok {
		return fmt.Errorf("not found column %v at view %v", orderBy, details.View.Name)
	}

	selector.OrderBy = col.Name
	return nil
}

func orderByValue(ctx context.Context, inputFormat format.Case, details *ViewDetails, params *RequestParams, ns string, metadata *RequestMetadata, selector *view.Selector) (string, error) {
	param := details.View.Selector.OrderByParam
	if param == nil {
		return params.queryParam(ns+string(OrderBy), ""), nil
	}

	value, err := extractParamValue(ctx, param, details, inputFormat, params, metadata, selector)
	if err != nil {
		return "", err
	}

	if actual, ok := value.(string); ok {
		return actual, nil
	}
	return "", typeMismatchError(param, value)
}

func populateOffset(ctx context.Context, selector *view.Selector, inputFormat format.Case, details *ViewDetails, params *RequestParams, ns string, metadata *RequestMetadata, s *view.Selector) error {
	offset, err := offsetValue(ctx, details, inputFormat, params, metadata, ns, selector)
	if err != nil || offset == 0 {
		return err
	}

	if !details.View.Selector.Constraints.Offset {
		return fmt.Errorf("can't use offset on view %v", details.View.Name)
	}

	selector.Offset = offset
	return nil
}

func offsetValue(ctx context.Context, details *ViewDetails, inputFormat format.Case, params *RequestParams, metadata *RequestMetadata, ns string, selector *view.Selector) (int, error) {
	param := details.View.Selector.OffsetParam
	if param == nil {
		return parseInt(params.queryParam(ns+string(Offset), ""))
	}

	value, err := extractParamValue(ctx, param, details, inputFormat, params, metadata, selector)
	if err != nil {
		return 0, err
	}

	return asInt(value, param)
}

func asInt(value interface{}, param *view.Parameter) (int, error) {
	if actual, ok := value.(int); ok {
		return actual, nil
	}

	return 0, typeMismatchError(param, value)
}

func populateFields(ctx context.Context, selector *view.Selector, inputFormat format.Case, details *ViewDetails, params *RequestParams, ns string, metadata *RequestMetadata) error {
	fieldValue, separator, err := fieldRawValue(ctx, details, inputFormat, params, metadata, ns, selector)
	if err != nil {
		return err
	}

	if fieldValue == "" {
		return err
	}

	if err = buildFields(inputFormat, details.View, selector, fieldValue, separator); err != nil {
		return err
	}

	return nil
}

func fieldRawValue(ctx context.Context, details *ViewDetails, inputFormat format.Case, params *RequestParams, metadata *RequestMetadata, ns string, selector *view.Selector) (string, int32, error) {
	param := details.View.Selector.FieldsParam
	if param == nil {
		return params.queryParam(ns+string(Fields), ""), ValuesSeparator, nil
	}

	paramValue, err := extractParamValue(ctx, param, details, inputFormat, params, metadata, selector)
	if err != nil {
		return "", ValuesSeparator, err
	}

	if actual, ok := paramValue.(string); ok {
		separator := ValuesSeparator
		return actual, separator, nil
	}

	return "", ValuesSeparator, typeMismatchError(param, paramValue)
}

func extractParamValue(ctx context.Context, param *view.Parameter, details *ViewDetails, inputFormat format.Case, params *RequestParams, metadata *RequestMetadata, selector *view.Selector) (interface{}, error) {
	switch param.In.Kind {
	case view.DataViewKind:
		return viewParamValue(ctx, inputFormat, details, param, params, metadata)
	case view.PathKind:
		return convertAndTransform(ctx, params.pathVariable(param.In.Name, ""), param, selector)
	case view.QueryKind:
		return convertAndTransform(ctx, params.queryParam(param.In.Name, ""), param, selector)
	case view.RequestBodyKind:
		return params.requestBody, nil
	case view.EnvironmentKind:
		return convertAndTransform(ctx, os.Getenv(param.In.Name), param, selector)
	case view.HeaderKind:
		return convertAndTransform(ctx, params.header(param.In.Name), param, selector)
	case view.CookieKind:
		return convertAndTransform(ctx, params.cookie(param.In.Name), param, selector)
	}

	return nil, fmt.Errorf("unsupported param kind %v", param.In.Kind)
}

func convertAndTransform(ctx context.Context, raw string, param *view.Parameter, selector *view.Selector) (interface{}, error) {
	if param.Codec == nil {
		return converter.Convert(raw, param.Schema.Type(), "")
	}

	return param.Codec.Transform(ctx, raw, selector)
}

func buildSelectorParameters(ctx context.Context, selector *view.Selector, inputFormat format.Case, parent *ViewDetails, paramsPtr, presencePtr unsafe.Pointer, parameters []*view.Parameter, requestParams *RequestParams, requestMetadata *RequestMetadata) error {
	var err error
	for _, parameter := range parameters {
		switch parameter.In.Kind {
		case view.QueryKind:
			if err = addQueryParam(ctx, selector, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case view.PathKind:
			if err = addPathParam(ctx, selector, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case view.HeaderKind:
			if err = addHeaderParam(ctx, selector, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case view.CookieKind:
			if err = addCookieParam(ctx, selector, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case view.DataViewKind:
			if err = addViewParam(ctx, selector, inputFormat, parent, paramsPtr, presencePtr, parameter, requestParams, requestMetadata); err != nil {
				return err
			}

		case view.RequestBodyKind:
			if err = addRequestBodyParam(selector, paramsPtr, presencePtr, parameter, requestParams); err != nil {
				return err
			}

		case view.EnvironmentKind:
			if err = addEnvVariableParam(ctx, selector, paramsPtr, presencePtr, parameter); err != nil {
				return err
			}
		}
	}
	return nil
}

func addEnvVariableParam(ctx context.Context, selector *view.Selector, paramsPtr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *view.Parameter) error {
	return convertAndSet(ctx, selector, paramsPtr, presencePtr, parameter, os.Getenv(parameter.In.Name))
}

func addRequestBodyParam(selector *view.Selector, paramsPtr unsafe.Pointer, presencePtr unsafe.Pointer, param *view.Parameter, requestParams *RequestParams) error {
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

func addCookieParam(ctx context.Context, selector *view.Selector, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *view.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, selector, ptr, presencePtr, parameter, params.cookie(parameter.In.Name))
}

func addHeaderParam(ctx context.Context, selector *view.Selector, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *view.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, selector, ptr, presencePtr, parameter, params.header(parameter.In.Name))
}

func addQueryParam(ctx context.Context, selector *view.Selector, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *view.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, selector, ptr, presencePtr, parameter, params.queryParam(parameter.In.Name, ""))
}

func addPathParam(ctx context.Context, selector *view.Selector, ptr unsafe.Pointer, presencePtr unsafe.Pointer, parameter *view.Parameter, params *RequestParams) error {
	return convertAndSet(ctx, selector, ptr, presencePtr, parameter, params.pathVariable(parameter.In.Name, ""))
}

func addViewParam(ctx context.Context, selector *view.Selector, inputFormat format.Case, viewDetails *ViewDetails, paramsPtr, presencePtr unsafe.Pointer, param *view.Parameter, params *RequestParams, requestMetadata *RequestMetadata) error {
	paramValue, err := viewParamValue(ctx, inputFormat, viewDetails, param, params, requestMetadata)
	if err != nil {
		return err
	}

	if paramValue == nil {
		return nil
	}

	if err = param.Set(paramsPtr, paramValue); err != nil {
		return err
	}

	param.UpdatePresence(presencePtr)
	return nil
}

func viewParamValue(ctx context.Context, inputFormat format.Case, viewDetails *ViewDetails, param *view.Parameter, params *RequestParams, requestMetadata *RequestMetadata) (interface{}, error) {
	aView := param.View()
	destSlice := reflect.New(aView.Schema.SliceType()).Interface()
	session := reader.NewSession(destSlice, aView)
	session.Parent = viewDetails.View
	newIndex := Index{}
	if err := newIndex.Init(aView, ""); err != nil {
		return nil, err
	}

	newRequestMetadata := &RequestMetadata{
		URI:      requestMetadata.URI,
		Index:    newIndex,
		MainView: nil,
	}

	selectors, err := CreateSelectors(ctx, inputFormat, newRequestMetadata, params, &ViewDetails{View: aView})
	if err != nil {
		return nil, err
	}

	session.Selectors = selectors
	if err = reader.New().Read(ctx, session); err != nil {
		return nil, err
	}
	ptr := xunsafe.AsPointer(destSlice)
	paramLen := aView.Schema.Slice().Len(ptr)
	switch paramLen {
	case 0:
		if param.Required != nil && *param.Required {
			return nil, fmt.Errorf("parameter %v value is required but no view was found", param.Name)
		}

		return nil, err
	case 1:
		holder := aView.Schema.Slice().ValuePointerAt(ptr, 0)
		return holder, nil
	default:
		return nil, fmt.Errorf("parameter %v return more than one value", param.Name)
	}
}

func convertAndSet(ctx context.Context, selector *view.Selector, paramPtr, presencePtr unsafe.Pointer, parameter *view.Parameter, rawValue string) error {
	if parameter.IsRequired() && rawValue == "" {
		return fmt.Errorf("parameter %v is required", parameter.Name)
	}

	if rawValue == "" {
		return nil
	}

	if err := parameter.ConvertAndSet(ctx, paramPtr, rawValue, selector); err != nil {
		return err
	}

	parameter.UpdatePresence(presencePtr)
	return nil
}

//TODO: Distinct fields
func buildFields(inputFormat format.Case, aView *view.View, selector *view.Selector, fieldsQuery string, separator int32) error {
	fieldIt := NewParamIt(fieldsQuery, separator)
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

func typeMismatchError(param *view.Parameter, value interface{}) error {
	return fmt.Errorf("parameter %v value type missmatch, wanted %v but got %T", param.Name, param.Schema.Type().String(), value)
}
