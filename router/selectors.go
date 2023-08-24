package router

import (
	"context"
	"fmt"
	"github.com/viant/datly/converter"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router/criteria"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	vstate "github.com/viant/datly/view/state"
	"github.com/viant/structology"
	"github.com/viant/toolbox/format"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xunsafe"
	"net/http"
	"os"
	"reflect"
	"sync"
	"unsafe"
)

type (
	RequestMetadata struct {
		URI      string
		Index    Index
		MainView *view.View
	}

	paramStateBuilder struct {
		resource        *view.Resource
		caser           format.Case
		dateFormat      string
		requestMetadata *RequestMetadata
		params          *RequestParams
		cache           *paramsValueCache
		viewParams      vstate.NamedParameters
	}

	JSONError struct {
		Message string
		Object  interface{}
	}

	paramsValueCache struct {
		index sync.Map
	}

	paramsValueKey struct {
		name     string
		target   string
		location vstate.Kind
	}

	paramValue struct {
		once   sync.Once
		value  interface{}
		err    error
		valuer func() (interface{}, error)
	}
)

func (e *JSONError) Error() string {
	return e.Message
}

func (b *paramStateBuilder) Build(ctx context.Context, viewsDetails []*ViewDetails, selectors *view.States) error {
	wg := sync.WaitGroup{}
	errors := httputils.NewErrors()

	var options []interface{}

	for _, details := range viewsDetails {
		selector := selectors.Lookup(details.View)
		selector.OutputFormat = b.caser
		selector.DatabaseFormat = details.View.Caser

		wg.Add(1)
		go func(ctx context.Context, details *ViewDetails, selector *view.State) {
			defer wg.Done()
			if paramName, err := b.populateSelector(ctx, selector, details); err != nil {
				errors.AddError(details.View.Name, paramName, err)
				return
			}

			if details.View.Template == nil || len(details.View.Template.Parameters) == 0 {
				return
			}
			if param, err := b.buildSelectorParameters(ctx, selector.Template, details, details.View.Template.Parameters, options...); err != nil {
				asErrors, ok := err.(*httputils.Errors)
				if param.ErrorStatusCode != 0 {
					errors.SetStatus(param.ErrorStatusCode)
				} else if ok && asErrors.ErrorStatusCode() != http.StatusBadRequest {
					errors.SetStatus(asErrors.ErrorStatusCode())
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
		return nil
	}

	return errors
}

func validateSelector(selector *view.State, aView *view.View) error {
	if selector.Offset != 0 && selector.Limit == 0 && aView.Selector.Limit == 0 {
		return fmt.Errorf("can't use offset if limit was not specified")
	}

	return nil
}

func BuildRouteSelectors(ctx context.Context, selectors *view.States, route *Route, request *http.Request) error {
	requestMetadata := NewRequestMetadata(route)
	requestParams, err := NewRequestParameters(request, route)
	if err != nil {
		return err
	}

	if requestParams == nil {
		var err error
		requestParams, err = NewRequestParameters(request, route)
		if err != nil {
			return err
		}
	}
	return CreateSelectors(ctx, route._resource, route.DateFormat, *route._caser, requestMetadata, requestParams, selectors, vstate.NamedParameters{}, route.Index._viewDetails...)
}

func CreateSelectorsFromRoute(ctx context.Context, route *Route, request *http.Request, requestParams *RequestParams, views ...*ViewDetails) (*view.States, *RequestParams, error) {
	requestMetadata := NewRequestMetadata(route)

	if requestParams == nil {
		var err error
		requestParams, err = NewRequestParameters(request, route)
		if err != nil {
			return nil, nil, err
		}
	}

	selectors := view.NewStates()
	if err := CreateSelectors(ctx, route._resource, route.DateFormat, *route._caser, requestMetadata, requestParams, selectors, nil, views...); err != nil {
		return nil, nil, err
	}
	return selectors, requestParams, nil
}

func NewRequestMetadata(route *Route) *RequestMetadata {
	requestMetadata := &RequestMetadata{
		URI:      route.URI,
		Index:    route.Index,
		MainView: route.View,
	}

	return requestMetadata
}

func CreateSelectors(ctx context.Context, resource *view.Resource, dateFormat string, inputFormat format.Case, requestMetadata *RequestMetadata, requestParams *RequestParams, selectors *view.States, paramsIndex vstate.NamedParameters, views ...*ViewDetails) error {
	sb := newParamStateBuilder(resource, inputFormat, dateFormat, requestMetadata, requestParams, newParamsValueCache(), paramsIndex)
	return sb.Build(ctx, views, selectors)
}

func newParamStateBuilder(resource *view.Resource, inputFormat format.Case, dateFormat string, requestMetadata *RequestMetadata, requestParams *RequestParams, cache *paramsValueCache, paramsIndex vstate.NamedParameters) *paramStateBuilder {
	sb := &paramStateBuilder{
		resource:        resource,
		caser:           inputFormat,
		dateFormat:      dateFormat,
		requestMetadata: requestMetadata,
		params:          requestParams,
		cache:           cache,
		viewParams:      paramsIndex,
	}
	return sb
}

func (b *paramStateBuilder) populateSelector(ctx context.Context, selector *view.State, details *ViewDetails) (string, error) {
	if details.View.Selector.FieldsParameter != nil {
		if err := b.populateFields(ctx, selector, details); err != nil {
			return view.FieldsQuery, err
		}
	}

	if details.View.Selector.LimitParameter != nil {
		if err := b.populateLimit(ctx, selector, details); err != nil {
			return view.LimitQuery, err
		}
	}

	if details.View.Selector.OffsetParameter != nil {
		if err := b.populateOffset(ctx, selector, details); err != nil {
			return view.OffsetQuery, err
		}
	}

	if details.View.Selector.OrderByParameter != nil {
		if err := b.populateOrderBy(ctx, selector, details); err != nil {
			return view.OrderByQuery, err
		}
	}

	if details.View.Selector.CriteriaParameter != nil {
		if err := b.populateCriteria(ctx, selector, details); err != nil {
			return view.CriteriaQuery, err
		}
	}

	if details.View.Selector.PageParameter != nil {
		if err := b.populatePage(ctx, selector, details); err != nil {
			return view.PageQuery, err
		}
	}

	if selector.Limit == 0 && selector.Offset != 0 {
		return "", fmt.Errorf("can't use offset without limit")
	}
	return "", nil
}

func (b *paramStateBuilder) populateCriteria(ctx context.Context, selector *view.State, details *ViewDetails) error {
	criteriaExpression, err := b.criteriaValue(ctx, details, selector)
	if err != nil || criteriaExpression == nil {
		return err
	}

	switch actual := criteriaExpression.(type) {
	case string:
		if err != nil || criteriaExpression == "" {
			return err
		}

		if !details.View.Selector.Constraints.Criteria {
			return fmt.Errorf("can't use criteria on view %v", details.View.Name)
		}

		sanitizedCriteria, err := criteria.Parse(actual, details.View.IndexedColumns(), details.View.Selector.Constraints.SqlMethodsIndexed())
		if err != nil {
			return err
		}

		selector.SetCriteria(sanitizedCriteria.Expression, sanitizedCriteria.Placeholders)
		return nil

	case *codec.Criteria:
		if actual == nil {
			return nil
		}

		selector.SetCriteria(actual.Predicate, actual.Args)
		return nil
	case codec.Criteria:
		selector.SetCriteria(actual.Predicate, actual.Args)
		return nil
	}

	return typeMismatchError(details.View.Selector.CriteriaParameter, criteriaExpression)
}

func (b *paramStateBuilder) criteriaValue(ctx context.Context, details *ViewDetails, selector *view.State) (interface{}, error) {
	param := details.View.Selector.CriteriaParameter
	return b.extractParamValue(ctx, param, details, selector)
}

func (b *paramStateBuilder) populateLimit(ctx context.Context, selector *view.State, details *ViewDetails) error {
	limit, err := b.limitValue(ctx, details, selector)
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

func (b *paramStateBuilder) limitValue(ctx context.Context, details *ViewDetails, selector *view.State) (int, error) {
	param := details.View.Selector.LimitParameter
	paramValue, err := b.extractParamValue(ctx, param, details, selector)
	if err != nil {
		return 0, err
	}

	return asInt(paramValue, param)
}

func (b *paramStateBuilder) populateOrderBy(ctx context.Context, selector *view.State, details *ViewDetails) error {
	orderBy, err := b.orderByValue(ctx, details, selector)
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

func (b *paramStateBuilder) orderByValue(ctx context.Context, details *ViewDetails, selector *view.State) (string, error) {
	param := details.View.Selector.OrderByParameter
	value, err := b.extractParamValue(ctx, param, details, selector)
	if err != nil || value == nil {
		return "", err
	}

	if actual, ok := value.(string); ok {
		return actual, nil
	}
	return "", typeMismatchError(param, value)
}

func (b *paramStateBuilder) populateOffset(ctx context.Context, selector *view.State, details *ViewDetails) error {
	offset, err := b.offsetValue(ctx, details, selector)
	if err != nil || offset == 0 {
		return err
	}

	if !details.View.Selector.Constraints.Offset {
		return fmt.Errorf("can't use offset on view %v", details.View.Name)
	}

	selector.Offset = offset
	return nil
}

func (b *paramStateBuilder) offsetValue(ctx context.Context, details *ViewDetails, selector *view.State) (int, error) {
	param := details.View.Selector.OffsetParameter
	value, err := b.extractParamValue(ctx, param, details, selector)
	if err != nil {
		return 0, err
	}

	return asInt(value, param)
}

func asInt(value interface{}, param *vstate.Parameter) (int, error) {
	if value == nil {
		return 0, nil
	}

	if actual, ok := value.(int); ok {
		return actual, nil
	}

	return 0, typeMismatchError(param, value)
}

func (b *paramStateBuilder) populateFields(ctx context.Context, selector *view.State, details *ViewDetails) error {
	fieldValue, err := b.fieldRawValue(ctx, details, selector)
	if err != nil || len(fieldValue) == 0 {
		return err
	}

	if len(fieldValue) > 0 && !details.View.Selector.Constraints.Projection {
		return fmt.Errorf("can't use projection on view %v", details.View.Name)
	}

	if err = b.buildFields(details.View, selector, fieldValue); err != nil {
		return err
	}

	return nil
}

func (b *paramStateBuilder) fieldRawValue(ctx context.Context, details *ViewDetails, selector *view.State) ([]string, error) {
	param := details.View.Selector.FieldsParameter
	paramValue, err := b.extractParamValue(ctx, param, details, selector)

	if err != nil || paramValue == nil {
		return []string{}, err
	}

	if actual, ok := paramValue.([]string); ok {
		return actual, nil
	}
	return nil, typeMismatchError(param, paramValue)
}

func (b *paramStateBuilder) extractParamValue(ctx context.Context, param *vstate.Parameter, details *ViewDetails, selector *view.State) (interface{}, error) {
	var options []interface{}
	if selector != nil {
		options = append(options, codec.Selector(selector))
	}

	if details != nil && details.View != nil {
		options = append(options, codec.ColumnsSource(details.View.IndexedColumns()))
	}

	options = append(options, codec.ValueGetter(b))

	return b.extractParamValueWithOptions(ctx, param, details.View, options...)
}

func (b *paramStateBuilder) lookupValue(ctx context.Context, name string) (interface{}, error) {
	aParameter, err := b.getParameter(name)
	if err != nil {
		return nil, err
	}
	value, err := b.params.extractHttpParam(context.Background(), aParameter, []interface{}{})
	if aParameter.Output == nil || err != nil {
		return value, err
	}
	return aParameter.Output.Transform(context.Background(), value)
}

func (b *paramStateBuilder) getParameter(name string) (*vstate.Parameter, error) {
	var aParameter *vstate.Parameter
	if b.resource != nil {
		var err error
		if aParameter, err = b.resource.LookupParameter(name); err != nil {
			return nil, err
		}
	}
	if aParameter == nil {
		aParameter, _ = b.viewParams[name]
		if aParameter == nil {
			return nil, fmt.Errorf("failed to lookup parameter: %s", name)
		}
	}
	return aParameter, nil
}

func (b *paramStateBuilder) extractParamValueWithOptions(ctx context.Context, param *vstate.Parameter, parentView *view.View, options ...interface{}) (interface{}, error) {

	value, err := b.cache.paramValue(param, func() (interface{}, error) {
		switch param.In.Kind {
		case vstate.KindDataView:
			return b.viewParamValue(ctx, param, parentView)
		case vstate.KindEnvironment:
			return os.Getenv(param.In.Name), nil
		case vstate.KindParam:
			return b.paramBasedParamValue(ctx, parentView, param, options...)
		case vstate.KindLiteral:
			return param.Const, nil
		case vstate.KindRequest:
			return b.params.request, nil
		case vstate.KindGroup:
			return b.groupParam(ctx, param, parentView)
		}
		return b.params.extractHttpParam(ctx, param, options)
	})

	if value == nil || err != nil {
		return nil, err
	}

	if param.Output == nil {
		return value, nil
	}
	codecOptions := vstate.AsCodecOptions(options)
	codecOptions = append(codecOptions, codec.WithValueLookup(b.lookupValue))
	return param.Output.Transform(ctx, value, codecOptions...)
}

func (p *RequestParams) convert(isSpecified bool, raw string, param *vstate.Parameter) (interface{}, error) {
	if raw == "" && param.IsRequired() {
		return nil, requiredParamErr(param)
	}

	if !isSpecified {
		return nil, nil
	}

	dateFormat := p.route.DateFormat
	if param.DateFormat != "" {
		dateFormat = param.DateFormat
	}

	convert, _, err := converter.Convert(raw, param.Schema.Type(), true, dateFormat)
	return convert, err
}

func (b *paramStateBuilder) buildSelectorParameters(ctx context.Context, aState *structology.State, parent *ViewDetails, parameters []*vstate.Parameter, options ...interface{}) (*vstate.Parameter, error) {
	var viewParams []*vstate.Parameter
	for _, parameter := range parameters {
		if parameter.In.Kind == vstate.KindDataView && parameter.ErrorStatusCode <= 400 {
			viewParams = append(viewParams, parameter)
			continue
		}

		err := b.handleParam(ctx, aState, parent, parameter, options...)
		if err != nil {
			return parameter, err
		}

		value, err := parameter.Value(aState)
		if err != nil {
			return parameter, err
		}
		if parameter.IsRequired() && isNull(value) {
			return parameter, requiredParamErr(parameter)
		}
	}

	if len(viewParams) > 0 {
		wg := &sync.WaitGroup{}
		mux := &sync.Mutex{}

		var invalidParam *vstate.Parameter
		var errParam error

		for _, param := range viewParams {
			wg.Add(1)
			go func(param *vstate.Parameter, wg *sync.WaitGroup) {
				defer wg.Done()
				err := b.handleParam(ctx, aState, parent, param, options...)
				if err != nil {
					mux.Lock()
					defer mux.Unlock()
					invalidParam = param
					errParam = err
				}
			}(param, wg)
		}

		wg.Wait()
		return invalidParam, errParam
	}

	return nil, nil
}

func isNull(value interface{}) bool {
	if value == nil {
		return true
	}

	return xunsafe.AsPointer(value) == nil
}

func (b *paramStateBuilder) handleParam(ctx context.Context, aState *structology.State, parent *ViewDetails, parameter *vstate.Parameter, options ...interface{}) error {
	var parentView *view.View
	if parent != nil {
		parentView = parent.View
	}
	value, err := b.extractParamValueWithOptions(ctx, parameter, parentView, options...)
	fmt.Printf("%T %+v\n", value, value)
	if err != nil {
		return err
	}

	if parameter.IsRequired() && value == nil {
		return requiredParamErr(parameter)
	}
	if value != nil {
		if err = parameter.Selector().SetValue(aState.Pointer(), value); err != nil {
			return err
		}
	}

	return nil
}

func requiredParamErr(param *vstate.Parameter) error {
	return fmt.Errorf("parameter %v is required", param.Name)
}

func (b *paramStateBuilder) viewParamValue(ctx context.Context, param *vstate.Parameter, parentView *view.View) (interface{}, error) {

	aView, _ := b.resource.View(param.In.Name)

	sliceType := aView.Schema.SliceType()
	slice := aView.Schema.Slice()
	var returnMulti bool
	if param.OutputType().Kind() == reflect.Slice {
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
	selectors := view.NewStates()

	if err := CreateSelectors(ctx, nil, b.dateFormat, b.caser, newRequestMetadata, b.params, selectors, b.viewParams, &ViewDetails{View: aView}); err != nil {
		return nil, err
	}

	session, err := reader.NewSession(destSlicePtr, aView, reader.WithParent(parentView))
	if err != nil {
		return nil, err
	}
	session.States = selectors
	if err := reader.New().Read(ctx, session); err != nil {
		return nil, err
	}
	ptr := xunsafe.AsPointer(destSlicePtr)
	paramLen := slice.Len(ptr)

	if param.MinAllowedRecords != nil && *param.MinAllowedRecords > paramLen {
		return nil, &JSONError{
			Object:  destSlicePtr,
			Message: fmt.Sprintf("expected to return at least %v records, but returned %v", *param.MinAllowedRecords, paramLen),
		}
	}

	if param.ExpectedReturned != nil && *param.ExpectedReturned != paramLen {
		return nil, &JSONError{
			Object:  destSlicePtr,
			Message: fmt.Sprintf("expected to return %v records, but returned %v", *param.ExpectedReturned, paramLen),
		}
	}

	if param.MaxAllowedRecords != nil && *param.MaxAllowedRecords < paramLen {
		return nil, &JSONError{
			Object:  destSlicePtr,
			Message: fmt.Sprintf("expected to return no more than %v records, but returned %v", *param.MaxAllowedRecords, paramLen),
		}
	}

	if paramLen == 0 && param.IsRequired() {
		return nil, fmt.Errorf("parameter %v value is required but no data was found", param.Name)
	}

	return b.paramViewValue(param, sliceValue, returnMulti, paramLen, slice, ptr)
}

func (b *paramStateBuilder) buildFields(aView *view.View, selector *view.State, fieldsQuery []string) error {
	for _, param := range fieldsQuery {
		fieldName := b.caser.Format(param, format.CaseUpperCamel)
		if err := canUseColumn(aView, fieldName); err != nil {
			return err
		}
		selector.Add(fieldName, aView.IsHolder(fieldName))
	}

	return nil
}

func (b *paramStateBuilder) paramViewValue(param *vstate.Parameter, value reflect.Value, multi bool, paramLen int, aSlice *xunsafe.Slice, ptr unsafe.Pointer) (interface{}, error) {
	if multi {
		return value.Elem().Interface(), nil
	}

	switch paramLen {
	case 0:
		return reflect.New(aSlice.Type.Elem()).Elem().Interface(), nil
	case 1:
		return aSlice.ValuePointerAt(ptr, 0), nil
	default:
		return nil, fmt.Errorf("parameter %v return more than one value, len: %v rows ", param.Name, paramLen)
	}
}

func (b *paramStateBuilder) populatePage(ctx context.Context, selector *view.State, details *ViewDetails) error {
	pageParam := details.View.Selector.PageParameter
	value, err := b.extractParamValue(ctx, pageParam, details, selector)
	if err != nil {
		return err
	}

	page, err := asInt(value, pageParam)
	if err != nil || page == 0 {
		return err
	}

	actualLimit := selector.Limit
	if actualLimit == 0 {
		actualLimit = details.View.Selector.Limit
	}

	selector.Offset = actualLimit * (page - 1)
	selector.Limit = actualLimit
	selector.Page = page
	return nil
}

func (b *paramStateBuilder) paramBasedParamValue(ctx context.Context, parentView *view.View, param *vstate.Parameter, options ...interface{}) (interface{}, error) {
	parent := param.Parent()
	value, err := b.extractParamValueWithOptions(ctx, parent, parentView, options...)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (b *paramStateBuilder) Value(ctx context.Context, paramName string) (interface{}, error) {
	lookup, err := b.viewParams.Lookup(paramName)
	if err != nil {
		return nil, err
	}

	return b.extractParamValueWithOptions(ctx, lookup, nil)
}

func (b *paramStateBuilder) groupParam(ctx context.Context, param *vstate.Parameter, parentView *view.View) (interface{}, error) {
	var aState *structology.State
	var value interface{}

	for _, p := range param.Group {
		pValue, err := b.extractParamValueWithOptions(ctx, p, parentView)
		if err != nil {
			return nil, err
		}

		if pValue != nil {
			if value == nil {
				value = types.NewValue(param.Schema.Type())
				aState = param.NewState(value)
			}

			if err := aState.SetValue(p.Name, pValue); err != nil {
				return nil, err
			}
		}
	}

	return value, nil
}

func canUseColumn(aView *view.View, columnName string) error {
	_, ok := aView.ColumnByName(columnName)
	if !ok {
		return fmt.Errorf("not found column %v in view %v", columnName, aView.Name)
	}
	return nil
}

func typeMismatchError(param *vstate.Parameter, value interface{}) error {
	return fmt.Errorf("parameter %v value type missmatch, wanted %v but got %T", param.Name, param.Schema.Type().String(), value)
}

func newParamsValueCache() *paramsValueCache {
	return &paramsValueCache{
		index: sync.Map{},
	}
}

func (p *paramsValueCache) paramValue(param *vstate.Parameter, valuer func() (interface{}, error)) (interface{}, error) {
	actual, _ := p.index.LoadOrStore(paramsValueKey{
		name:     param.Name,
		target:   param.In.Name,
		location: param.In.Kind,
	}, &paramValue{
		valuer: valuer,
		once:   sync.Once{},
	})

	value := actual.(*paramValue)
	return value.get()
}

func (v *paramValue) get() (interface{}, error) {
	v.once.Do(v.init)
	return v.value, v.err
}

func (v *paramValue) init() {
	v.value, v.err = v.valuer()
}
