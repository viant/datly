package router

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/criteria"
	"github.com/viant/datly/view"
	"github.com/viant/toolbox/format"
	"github.com/viant/xunsafe"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

type (
	RequestMetadata struct {
		URI      string
		Index    Index
		MainView *view.View
	}

	selectorsBuilder struct {
		caser           format.Case
		dateFormat      string
		requestMetadata *RequestMetadata
		params          *RequestParams
	}

	JSONError struct {
		Object interface{}
	}
)

func (e *JSONError) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.Object)
}

func (e *JSONError) Error() string {
	marshal, _ := json.Marshal(e.Object)
	return string(marshal)
}

func (b *selectorsBuilder) build(ctx context.Context, viewsDetails []*ViewDetails) (*view.Selectors, error) {
	selectors := view.Selectors{}
	wg := sync.WaitGroup{}
	errors := NewErrors()
	for _, details := range viewsDetails {
		selector := selectors.Lookup(details.View)
		selector.OutputFormat = b.caser
		selector.DatabaseFormat = details.View.Caser

		wg.Add(1)
		go func(ctx context.Context, details *ViewDetails, selector *view.Selector) {
			defer wg.Done()
			if paramName, err := b.populateSelector(ctx, selector, details); err != nil {
				errors.AddError(details.View.Name, paramName, err)
				return
			}

			if details.View.Template == nil || len(details.View.Template.Parameters) == 0 {
				return
			}

			selector.Parameters.Init(details.View)
			if param, err := b.buildSelectorParameters(ctx, selector, details, details.View.Template.Parameters); err != nil {
				if param.ErrorStatusCode != 0 {
					errors.setStatus(param.ErrorStatusCode)
				}

				errors.AddError(details.View.Name, param.Name, err)
				return
			}

			if err := validateSelector(selector, details.View); err != nil {
				errors.AddError(details.View.Name, "", err)
				return
			}
		}(ctx, details, selector)
	}

	wg.Wait()
	if len(errors.Errors) == 0 {
		return &selectors, nil
	}

	return nil, errors
}

func validateSelector(selector *view.Selector, aView *view.View) error {
	if selector.Offset != 0 && selector.Limit == 0 && aView.Selector.Limit == 0 {
		return fmt.Errorf("can't use offset if limit was not specified")
	}

	return nil
}

func CreateSelectorsFromRoute(ctx context.Context, route *Route, request *http.Request, views ...*ViewDetails) (*view.Selectors, error) {
	requestMetadata := NewRequestMetadata(route)
	requestParams, err := NewRequestParameters(request, route)
	if err != nil {
		return nil, err
	}

	return CreateSelectors(ctx, route.DateFormat, *route._caser, requestMetadata, requestParams, views...)
}

func NewRequestMetadata(route *Route) *RequestMetadata {
	requestMetadata := &RequestMetadata{
		URI:      route.URI,
		Index:    route.Index,
		MainView: route.View,
	}

	return requestMetadata
}

func CreateSelectors(ctx context.Context, dateFormat string, inputFormat format.Case, requestMetadata *RequestMetadata, requestParams *RequestParams, views ...*ViewDetails) (*view.Selectors, error) {
	sb := &selectorsBuilder{
		caser:           inputFormat,
		dateFormat:      dateFormat,
		requestMetadata: requestMetadata,
		params:          requestParams,
	}

	return sb.build(ctx, views)
}

func (b *selectorsBuilder) populateSelector(ctx context.Context, selector *view.Selector, details *ViewDetails) (string, error) {
	for i, ns := range details.Prefixes {
		if i == 0 || details.View.Selector.FieldsParam == nil {
			if err := b.populateFields(ctx, selector, details, ns); err != nil {
				return string(Fields), err
			}
		}

		if i == 0 || details.View.Selector.LimitParam == nil {
			if err := b.populateLimit(ctx, selector, details, ns); err != nil {
				return string(Limit), err
			}
		}

		if i == 0 || details.View.Selector.OffsetParam == nil {
			if err := b.populateOffset(ctx, selector, details, ns); err != nil {
				return string(Offset), err
			}
		}

		if i == 0 || details.View.Selector.OrderByParam == nil {
			if err := b.populateOrderBy(ctx, selector, details, ns); err != nil {
				return string(OrderBy), err
			}
		}

		if i == 0 || details.View.Selector.CriteriaParam == nil {
			if err := b.populateCriteria(ctx, selector, details, ns); err != nil {
				return string(Criteria), err
			}
		}
	}

	return "", nil
}

func (b *selectorsBuilder) populateCriteria(ctx context.Context, selector *view.Selector, details *ViewDetails, ns string) error {
	criteriaExpression, err := b.criteriaValue(ctx, details, ns, selector)
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

func (b *selectorsBuilder) criteriaValue(ctx context.Context, details *ViewDetails, ns string, selector *view.Selector) (string, error) {
	param := details.View.Selector.CriteriaParam
	if param == nil {
		return b.params.queryParam(ns+string(Criteria), ""), nil
	}

	paramValue, err := b.extractParamValue(ctx, param, details, selector)
	if err != nil || paramValue == nil {
		return "", err
	}

	if actual, ok := paramValue.(string); ok {
		return actual, nil
	}

	return "", typeMismatchError(param, paramValue)
}

func (b *selectorsBuilder) populateLimit(ctx context.Context, selector *view.Selector, details *ViewDetails, ns string) error {
	limit, err := b.limitValue(ctx, details, ns, selector)
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

func (b *selectorsBuilder) limitValue(ctx context.Context, details *ViewDetails, ns string, selector *view.Selector) (int, error) {
	param := details.View.Selector.LimitParam
	if param == nil {
		return parseInt(b.params.queryParam(ns+string(Limit), ""))
	}

	paramValue, err := b.extractParamValue(ctx, param, details, selector)
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

func (b *selectorsBuilder) populateOrderBy(ctx context.Context, selector *view.Selector, details *ViewDetails, ns string) error {
	orderBy, err := b.orderByValue(ctx, details, ns, selector)
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

func (b *selectorsBuilder) orderByValue(ctx context.Context, details *ViewDetails, ns string, selector *view.Selector) (string, error) {
	param := details.View.Selector.OrderByParam
	if param == nil {
		return b.params.queryParam(ns+string(OrderBy), ""), nil
	}

	value, err := b.extractParamValue(ctx, param, details, selector)
	if err != nil {
		return "", err
	}

	if actual, ok := value.(string); ok {
		return actual, nil
	}
	return "", typeMismatchError(param, value)
}

func (b *selectorsBuilder) populateOffset(ctx context.Context, selector *view.Selector, details *ViewDetails, ns string) error {
	offset, err := b.offsetValue(ctx, details, ns, selector)
	if err != nil || offset == 0 {
		return err
	}

	if !details.View.Selector.Constraints.Offset {
		return fmt.Errorf("can't use offset on view %v", details.View.Name)
	}

	selector.Offset = offset
	return nil
}

func (b *selectorsBuilder) offsetValue(ctx context.Context, details *ViewDetails, ns string, selector *view.Selector) (int, error) {
	param := details.View.Selector.OffsetParam
	if param == nil {
		pageValue := b.params.queryParam(ns+string(Page), "")
		if pageValue != "" {
			return b.pageOffset(pageValue, selector, details.View)
		}

		return parseInt(b.params.queryParam(ns+string(Offset), ""))
	}

	value, err := b.extractParamValue(ctx, param, details, selector)
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

func (b *selectorsBuilder) populateFields(ctx context.Context, selector *view.Selector, details *ViewDetails, ns string) error {
	fieldValue, separator, err := b.fieldRawValue(ctx, details, ns, selector)
	if err != nil {
		return err
	}

	if fieldValue == "" {
		return err
	}

	if fieldValue != "" && !details.View.Selector.Constraints.Projection {
		return fmt.Errorf("can't use projection on view %v", details.View.Name)
	}

	if err = b.buildFields(details.View, selector, fieldValue, separator); err != nil {
		return err
	}

	return nil
}

func (b *selectorsBuilder) fieldRawValue(ctx context.Context, details *ViewDetails, ns string, selector *view.Selector) (string, int32, error) {
	param := details.View.Selector.FieldsParam
	if param == nil {
		return b.params.queryParam(ns+string(Fields), ""), ValuesSeparator, nil
	}

	paramValue, err := b.extractParamValue(ctx, param, details, selector)
	if err != nil {
		return "", ValuesSeparator, err
	}

	if actual, ok := paramValue.(string); ok {
		separator := ValuesSeparator
		return actual, separator, nil
	}

	return "", ValuesSeparator, typeMismatchError(param, paramValue)
}

func (b *selectorsBuilder) extractParamValue(ctx context.Context, param *view.Parameter, details *ViewDetails, selector *view.Selector) (interface{}, error) {
	switch param.In.Kind {
	case view.DataViewKind:
		return b.viewParamValue(ctx, details, param)
	case view.PathKind:
		return b.convertAndTransform(ctx, b.params.pathVariable(param.In.Name, ""), param, selector)
	case view.QueryKind:
		return b.convertAndTransform(ctx, b.params.queryParam(param.In.Name, ""), param, selector)
	case view.RequestBodyKind:
		return b.params.requestBody, nil
	case view.EnvironmentKind:
		return b.convertAndTransform(ctx, os.Getenv(param.In.Name), param, selector)
	case view.HeaderKind:
		return b.convertAndTransform(ctx, b.params.header(param.In.Name), param, selector)
	case view.CookieKind:
		return b.convertAndTransform(ctx, b.params.cookie(param.In.Name), param, selector)
	}

	return nil, fmt.Errorf("unsupported param kind %v", param.In.Kind)
}

func (b *selectorsBuilder) convertAndTransform(ctx context.Context, raw string, param *view.Parameter, selector *view.Selector) (interface{}, error) {
	dateFormat := b.dateFormat
	if param.DateFormat != "" {
		dateFormat = param.DateFormat
	}

	if param.Codec == nil {
		convert, _, err := converter.Convert(raw, param.Schema.Type(), dateFormat)
		return convert, err
	}

	return param.Codec.Transform(ctx, raw, selector)
}

func (b *selectorsBuilder) buildSelectorParameters(ctx context.Context, selector *view.Selector, parent *ViewDetails, parameters []*view.Parameter) (*view.Parameter, error) {

	var err error
	for _, parameter := range parameters {
		err = b.handleParam(ctx, selector, parent, parameter)
		if err != nil {
			return parameter, err
		}
	}
	return nil, nil
}

func (b *selectorsBuilder) handleParam(ctx context.Context, selector *view.Selector, parent *ViewDetails, parameter *view.Parameter) error {
	switch parameter.In.Kind {
	case view.QueryKind:
		if err := b.addQueryParam(ctx, selector, parameter); err != nil {
			return err
		}

	case view.PathKind:
		if err := b.addPathParam(ctx, selector, parameter); err != nil {
			return err
		}

	case view.HeaderKind:
		if err := b.addHeaderParam(ctx, selector, parameter); err != nil {
			return err
		}

	case view.CookieKind:
		if err := b.addCookieParam(ctx, selector, parameter); err != nil {
			return err
		}

	case view.DataViewKind:
		if err := b.addViewParam(ctx, selector, parent, parameter); err != nil {
			return err
		}

	case view.RequestBodyKind:
		if err := b.addRequestBodyParam(ctx, selector, parameter); err != nil {
			return err
		}

	case view.EnvironmentKind:
		if err := b.addEnvVariableParam(ctx, selector, parameter); err != nil {
			return err
		}
	}

	return nil
}

func (b *selectorsBuilder) addEnvVariableParam(ctx context.Context, selector *view.Selector, parameter *view.Parameter) error {
	return convertAndSet(ctx, selector, parameter, os.Getenv(parameter.In.Name))
}

func (b *selectorsBuilder) addRequestBodyParam(ctx context.Context, selector *view.Selector, param *view.Parameter) error {
	if param.Required != nil && *param.Required && b.params.requestBody == nil {
		return fmt.Errorf("parameter %v is required", param.Name)
	}

	if b.params.requestBody == nil {
		return nil
	}

	if param.In.Name == "" {
		return param.Set(selector, b.params.requestBody)
	}

	bodyValue, ok := b.extractBody(param.In.Name)
	if !ok {
		return nil
	}

	return param.ConvertAndSet(ctx, selector, bodyValue)
}

func (b *selectorsBuilder) addCookieParam(ctx context.Context, selector *view.Selector, parameter *view.Parameter) error {
	return convertAndSet(ctx, selector, parameter, b.params.cookie(parameter.In.Name))
}

func (b *selectorsBuilder) addHeaderParam(ctx context.Context, selector *view.Selector, parameter *view.Parameter) error {
	return convertAndSet(ctx, selector, parameter, b.params.header(parameter.In.Name))
}

func (b *selectorsBuilder) addQueryParam(ctx context.Context, selector *view.Selector, parameter *view.Parameter) error {
	return convertAndSet(ctx, selector, parameter, b.params.queryParam(parameter.In.Name, ""))
}

func (b *selectorsBuilder) addPathParam(ctx context.Context, selector *view.Selector, parameter *view.Parameter) error {
	return convertAndSet(ctx, selector, parameter, b.params.pathVariable(parameter.In.Name, ""))
}

func (b *selectorsBuilder) addViewParam(ctx context.Context, selector *view.Selector, viewDetails *ViewDetails, param *view.Parameter) error {
	paramValue, err := b.viewParamValue(ctx, viewDetails, param)
	if err != nil {
		return err
	}

	if paramValue == nil {
		return nil
	}

	if err = param.Set(selector, paramValue); err != nil {
		return err
	}

	return nil
}

func (b *selectorsBuilder) viewParamValue(ctx context.Context, viewDetails *ViewDetails, param *view.Parameter) (interface{}, error) {
	aView := param.View()

	sliceType := aView.Schema.SliceType()
	slice := aView.Schema.Slice()
	var returnMulti bool
	if param.Schema.Type().Kind() == reflect.Slice {
		sliceType = param.Schema.Type()
		slice = param.Schema.Slice()
		returnMulti = true
	}

	sliceValue := reflect.New(sliceType)
	destSlicePtr := sliceValue.Interface()

	newIndex := Index{}
	if err := newIndex.Init(aView, ""); err != nil {
		return nil, err
	}

	newRequestMetadata := &RequestMetadata{
		URI:      b.requestMetadata.URI,
		Index:    newIndex,
		MainView: nil,
	}

	selectors, err := CreateSelectors(ctx, b.dateFormat, b.caser, newRequestMetadata, b.params, &ViewDetails{View: aView})
	if err != nil {
		return nil, err
	}

	session := reader.NewSession(destSlicePtr, aView, viewDetails.View)
	session.Selectors = selectors
	if err = reader.New().Read(ctx, session); err != nil {
		return nil, err
	}

	ptr := xunsafe.AsPointer(destSlicePtr)
	paramLen := slice.Len(ptr)
	if param.MaxAllowedRecords != nil && *param.MaxAllowedRecords < paramLen {
		return nil, &JSONError{Object: destSlicePtr}
	}

	if paramLen == 0 && param.IsRequired() {
		return nil, fmt.Errorf("parameter %v value is required but no data was found", param.Name)
	}

	return b.paramViewValue(param, sliceValue, returnMulti, paramLen, slice, ptr)
}

func convertAndSet(ctx context.Context, selector *view.Selector, parameter *view.Parameter, rawValue string) error {
	if parameter.IsRequired() && rawValue == "" {
		return fmt.Errorf("parameter %v is required", parameter.Name)
	}

	if rawValue == "" {
		return nil
	}

	if err := parameter.ConvertAndSet(ctx, selector, rawValue); err != nil {
		return err
	}

	return nil
}

func (b *selectorsBuilder) buildFields(aView *view.View, selector *view.Selector, fieldsQuery string, separator int32) error {
	fieldIt := NewParamIt(fieldsQuery, separator)
	for fieldIt.Has() {
		param, err := fieldIt.Next()
		if err != nil {
			return err
		}

		fieldName := b.caser.Format(param.Value, format.CaseUpperCamel)
		if err = canUseColumn(aView, fieldName); err != nil {
			return err
		}

		selector.Add(fieldName, aView.IsHolder(fieldName))
	}

	return nil
}

func (b *selectorsBuilder) pageOffset(pageValue string, selector *view.Selector, aView *view.View) (int, error) {
	page, err := strconv.Atoi(pageValue)
	if err != nil {
		return 0, err
	}

	limit := aView.Selector.Limit
	selector.Page = page

	if selector.Limit != 0 {
		limit = selector.Limit
	}

	if page <= 0 {
		return 0, fmt.Errorf("page can't be lower than 1")
	}

	return (page - 1) * limit, nil
}

func (b *selectorsBuilder) paramViewValue(param *view.Parameter, value reflect.Value, multi bool, paramLen int, aSlice *xunsafe.Slice, ptr unsafe.Pointer) (interface{}, error) {
	if multi {
		return value.Elem().Interface(), nil
	}

	switch paramLen {
	case 1:
		return aSlice.ValuePointerAt(ptr, 0), nil
	default:
		return nil, fmt.Errorf("parameter %v return more than one value", param.Name)
	}
}

func (b *selectorsBuilder) extractBody(path string) (string, bool) {
	segments := strings.Split(path, ".")

	var rawValue interface{} = b.params.bodyMap
	for _, segment := range segments {
		actualMap, ok := rawValue.(map[string]interface{})
		if !ok {
			return "", false
		}

		segmentValue, ok := actualMap[segment]
		if !ok {
			return "", false
		}

		rawValue = segmentValue
	}

	rawString, ok := rawValue.(string)
	if ok {
		return rawString, true
	}

	marshal, err := json.Marshal(rawValue)
	if err != nil {
		return "", false
	}

	return string(marshal), true
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
