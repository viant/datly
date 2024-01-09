package session

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/datly/internal/converter"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/datly/view/tags"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"unsafe"
)

type (
	Session struct {
		cache *cache
		views *views
		Options
		state.Types
	}

	contextKey string
)

func (s *Session) Value(ctx context.Context, key string) (interface{}, bool, error) {
	parameter, ok := s.namedParameters[key]
	if !ok {
		return nil, false, fmt.Errorf("unknwon state parameter: %v", key)
	}
	return s.lookupValue(ctx, parameter, s.Options.Indirect(true))
}

const _contextKey = contextKey("session")

// Context returns session context
func Context(ctx context.Context) *Session {
	if ctx == nil {
		return nil
	}
	value := ctx.Value(_contextKey)
	if value == nil {
		return nil
	}
	return value.(*Session)
}

// Context returns session context
func (s *Session) Context(ctx context.Context, forceNew bool) context.Context {
	if Context(ctx) != nil && !forceNew {
		return ctx
	}
	return context.WithValue(ctx, _contextKey, s)
}

// Populate populates view state
func (s *Session) Populate(ctx context.Context) error {
	if len(s.namespacedView.Views) == 0 {
		return nil
	}
	err := httputils.NewErrors()
	wg := sync.WaitGroup{}
	for i := range s.namespacedView.Views {
		wg.Add(1)
		aView := s.namespacedView.Views[i].View
		go s.setViewStateInBackground(ctx, aView, err, &wg)
	}
	wg.Wait()
	if !err.HasError() {
		return nil
	}
	return err
}

func (s *Session) setViewStateInBackground(ctx context.Context, aView *view.View, errors *httputils.Errors, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := s.SetViewState(ctx, aView); err != nil {
		errors.Append(err)
	}
}

// SetViewState sets view state as long state has not been populated
func (s *Session) SetViewState(ctx context.Context, aView *view.View) error {
	if !s.views.canPopulateView(aView.Name) { //already state populated
		return nil
	}
	return s.setViewState(ctx, aView)
}

// ResetViewState sets view state
func (s *Session) ResetViewState(ctx context.Context, aView *view.View) error {
	return s.setViewState(ctx, aView)
}

func (s *Session) setViewState(ctx context.Context, aView *view.View) (err error) {
	opts := s.ViewOptions(aView)
	if aView.Mode == view.ModeQuery {
		ns := s.viewNamespace(aView)
		if err = s.setQuerySelector(ctx, ns, opts); err != nil {
			return err
		}
	}
	if err = s.setTemplateState(ctx, aView, opts); err != nil {
		s.adjustErrorSource(err, aView)
		return err
	}

	if aView.Mode == view.ModeQuery {
		ns := s.viewNamespace(aView)
		if err = s.setQuerySettings(ctx, ns, opts); err != nil {
			return err
		}
	}
	return err
}

func (s *Session) viewNamespace(aView *view.View) *view.NamespaceView {
	ns := s.namespacedView.ByName(aView.Name)
	if ns == nil {
		ns = &view.NamespaceView{View: aView, Path: "", Namespaces: []string{aView.Selector.Namespace}}
	}
	return ns
}

func (s *Session) adjustErrorSource(err error, aView *view.View) {
	switch actual := err.(type) {
	case *httputils.Error:
		actual.View = aView.Name
	case *httputils.Errors:
		for _, item := range actual.Errors {
			item.View = aView.Name
		}
	}
}

func (s *Session) viewLookupOptions(aView *view.View, parameters state.NamedParameters, opts *Options) []locator.Option {
	var result []locator.Option
	result = append(result, locator.WithParameterLookup(func(ctx context.Context, parameter *state.Parameter) (interface{}, bool, error) {
		return s.LookupValue(ctx, parameter, opts)
	}))
	result = append(result, locator.WithInputParameters(parameters))
	result = append(result, locator.WithReadInto(s.ReadInto))
	viewState := s.state.Lookup(aView)
	result = append(result, locator.WithState(viewState.Template))

	return result
}

func (s *Session) ViewOptions(aView *view.View, opts ...Option) *Options {
	selectors := s.state.Lookup(aView)
	viewOptions := s.Options.Clone()
	var parameters state.NamedParameters
	if aView.Template != nil {
		parameters = aView.Template.Parameters.Index()
	}
	viewOptions.kindLocator = s.kindLocator.With(s.viewLookupOptions(aView, parameters, viewOptions)...)
	viewOptions.AddCodec(codec.WithSelector(codec.Selector(selectors)))
	viewOptions.AddCodec(codec.WithColumnsSource(aView.IndexedColumns()))

	getter := &valueGetter{Parameters: parameters, Session: s, Options: viewOptions}

	//TODO replace  with locator.ParameterLookup  option
	viewOptions.AddCodec(codec.WithValueGetter(getter))
	viewOptions.AddCodec(codec.WithValueLookup(getter.Value))
	//TODO end
	for _, opt := range opts {
		opt(viewOptions)
	}
	return viewOptions
}

func (s *Session) setTemplateState(ctx context.Context, aView *view.View, opts *Options) error {
	aState := s.state.Lookup(aView)
	if template := aView.Template; template != nil {
		stateType := template.StateType()
		if stateType.IsDefined() {
			templateState := aState.Template
			templateState.EnsureMarker()
			err := s.SetState(ctx, template.Parameters, templateState, opts)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Session) SetState(ctx context.Context, parameters state.Parameters, aState *structology.State, opts *Options) error {
	err := httputils.NewErrors()
	parametersGroup := parameters.GroupByStatusCode()
	for _, group := range parametersGroup {
		wg := sync.WaitGroup{}
		for i, _ := range group { //populate non data view parameters first
			wg.Add(1)
			parameter := group[i]
			go s.populateParameterInBackground(ctx, parameter, aState, opts, err, &wg)
		}
		wg.Wait()
		if err.HasError() {
			return err
		}
	}
	return nil
}

func (s *Session) populateParameterInBackground(ctx context.Context, parameter *state.Parameter, aState *structology.State, options *Options, errors *httputils.Errors, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := s.populateParameter(ctx, parameter, aState, options); err != nil {
		s.handleParameterError(parameter, err, errors)
	}
}

func (s *Session) populateParameter(ctx context.Context, parameter *state.Parameter, aState *structology.State, options *Options) error {

	value, has, err := s.LookupValue(ctx, parameter, options)
	if err != nil {
		return err
	}
	if !has {
		if parameter.IsRequired() {
			return fmt.Errorf("parameter %v is required", parameter.Name)
		}
		return nil
	}
	parameterSelector := parameter.Selector()
	if options.indirectState || parameterSelector == nil { //p
		parameterSelector, err = aState.Selector(parameter.Name)
		if parameter.In.Kind == state.KindConst {
			return nil
		}
		if err != nil {
			return err
		}
	}
	if value, err = s.ensureValidValue(value, parameter, parameterSelector, options); err != nil {
		return err
	}
	err = parameterSelector.SetValue(aState.Pointer(), value)
	return err
}

func (s *Session) canRead(ctx context.Context, parameter *state.Parameter) (bool, error) {
	if parameter.When == "" {
		return true, nil
	}
	//TODO move template based or IGO based expressions, for now qucick impl for equal only based check
	//move ast to build init time
	index := strings.Index(parameter.When, "=")
	if index == -1 {
		return false, fmt.Errorf("currently only basic check with = is supported ")
	}
	parameterName := parameter.When[:index]
	index = strings.LastIndex(parameter.When, "=")
	value := strings.TrimSpace(parameter.When[index+1:])
	if !strings.HasPrefix(parameterName, "$") {
		return false, fmt.Errorf("invalid expr, expected $parametr=value, but had: %v", parameterName)
	}
	name := parameterName[1:]
	parameterX := s.namedParameters[name]
	if parameterX == nil {
		return false, fmt.Errorf("failed to lookup parameter: %s", name)
	}
	x, has, err := s.lookupValue(ctx, parameterX, nil)
	if !has || err != nil {
		return false, err
	}

	shallPopulate, err := equals(x, value)
	return shallPopulate, err
}

func (s *Session) ensureValidValue(value interface{}, parameter *state.Parameter, selector *structology.Selector, options *Options) (interface{}, error) {
	if options == nil {
		options = &s.Options
	}
	parameterType := parameter.Schema.Type()
	if value == nil {
		switch parameterType.Kind() {
		case reflect.Ptr, reflect.Slice:
			return reflect.New(parameterType).Elem().Interface(), nil
		}
	}
	valueType := reflect.TypeOf(value)

	if valueType == nil {
		fmt.Printf("value type was nil %s\n", parameter.Name)
	}

	switch valueType.Kind() {
	case reflect.Ptr:
		if parameter.IsRequired() && isNil(value) {
			return nil, fmt.Errorf("parameter %v is required", parameter.Name)
		}
	case reflect.Slice:
		ptr := xunsafe.AsPointer(value)
		slice := parameter.Schema.Slice()
		sliceLen := slice.Len(ptr)
		if errorMessage := validateSliceParameter(parameter, sliceLen); errorMessage != "" {
			return nil, errors.New(errorMessage)
		}
		outputType := parameter.OutputType()
		switch outputType.Kind() {
		case reflect.Slice:

		default:
			switch sliceLen {
			case 0:
				value = reflect.New(parameter.OutputType().Elem()).Elem().Interface()
				valueType = reflect.TypeOf(value)
			case 1:
				value = slice.ValuePointerAt(ptr, 0)
				valueType = reflect.TypeOf(value)
			default:
				return nil, fmt.Errorf("parameter %v return more than one value, len: %v rows ", parameter.Name, sliceLen)
			}
		}
	}

	if valueType.Kind() != selector.Type().Kind() {
		if valueType.Kind() == reflect.Ptr && value != nil {
			valueType = valueType.Elem()
			value = reflect.ValueOf(value).Elem().Interface()
		}
	}

	if parameter.Schema.IsStruct() && !(valueType == selector.Type() || valueType.ConvertibleTo(selector.Type()) || valueType.AssignableTo(selector.Type())) {
		if options.shallReportNotAssignable() {
			fmt.Printf("parameter %v is not directly assignable from %s:(%s)\nsrc:%s \ndst:%s\n", parameter.Name, parameter.In.Kind, parameter.In.Name, valueType.String(), selector.Type().String())
		}

		reflectValue := reflect.New(valueType) //TODO replace with fast xreflect copy
		valuePtr := reflectValue.Interface()
		if data, err := json.Marshal(value); err == nil {
			if err = json.Unmarshal(data, valuePtr); err == nil {
				value = reflectValue.Elem().Interface()
			}
		}
	}
	return value, nil
}

func validateSliceParameter(parameter *state.Parameter, sliceLen int) string {
	errorMessage := ""
	if parameter.MinAllowedRecords != nil && *parameter.MinAllowedRecords > sliceLen {
		errorMessage = fmt.Sprintf("expected at least %v records, but had %v", *parameter.MinAllowedRecords, sliceLen)
	} else if parameter.ExpectedReturned != nil && *parameter.ExpectedReturned != sliceLen {
		errorMessage = fmt.Sprintf("expected  %v records, but had %v", *parameter.ExpectedReturned, sliceLen)
	} else if parameter.MaxAllowedRecords != nil && *parameter.MaxAllowedRecords < sliceLen {
		errorMessage = fmt.Sprintf("expected to no more than %v records, but had %v", *parameter.MaxAllowedRecords, sliceLen)
	} else if sliceLen == 0 && parameter.IsRequired() {
		errorMessage = fmt.Sprintf("parameter %v value is required but no data was found", parameter.Name)
	}
	return errorMessage
}

func isNil(value interface{}) bool {
	if ptr := xunsafe.AsPointer(value); (*unsafe.Pointer)(ptr) == nil {
		return true
	}
	return false
}

func (s *Session) lookupFirstValue(ctx context.Context, parameters []*state.Parameter, opts *Options) (value interface{}, has bool, err error) {
	for _, parameter := range parameters {
		value, has, err = s.LookupValue(ctx, parameter, opts)
		if has {
			return value, has, err
		}
	}
	return value, has, err
}

func (s *Session) LookupValue(ctx context.Context, parameter *state.Parameter, opts *Options) (value interface{}, has bool, err error) {

	if value, has, err = s.lookupValue(ctx, parameter, opts); err != nil {
		err = httputils.NewParamError("", parameter.Name, err, httputils.WithObject(value), httputils.WithStatusCode(parameter.ErrorStatusCode))
	}
	return value, has, err
}

var requestParameter = &state.Parameter{Name: "request", In: state.NewRequestLocation(), Schema: state.NewSchema(reflect.TypeOf(&http.Request{}))}

func (s *Session) HttpRequest(ctx context.Context, options *Options) (*http.Request, error) {
	value, has, err := s.LookupValue(ctx, requestParameter, options)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, nil
	}
	request, ok := value.(*http.Request)
	if !ok {
		return nil, fmt.Errorf("expected %T, but had %T", request, value)
	}
	return request, nil
}

// SetCacheValue sets cache value
func (s *Session) SetCacheValue(ctx context.Context, parameter *state.Parameter, value interface{}) error {
	_, _, err := s.adjustAndCache(ctx, parameter, &s.Options, true, value, true)
	return err
}

func (s *Session) lookupValue(ctx context.Context, parameter *state.Parameter, opts *Options) (value interface{}, has bool, err error) {
	if opts == nil {
		opts = &s.Options
	}
	canRead, err := s.canRead(ctx, parameter)
	if !canRead || err != nil {
		return nil, false, err
	}
	cachable := parameter.IsCacheable()
	if value, has = s.cache.lookup(parameter); has && cachable {
		return value, has, nil
	}
	lock := s.cache.lockParameter(parameter) //lockParameter is to ensure value for a parameter is computed only once
	lock.Lock()
	defer lock.Unlock()
	if value, has = s.cache.lookup(parameter); has && cachable {
		return value, has, nil
	}

	locators := opts.kindLocator
	switch parameter.In.Kind {
	case state.KindConst:
		value, has = parameter.Value, true
	default:
		parameterLocator, err := locators.Lookup(parameter.In.Kind)
		if err != nil {
			return nil, false, fmt.Errorf("failed to locate parameter: %v, %w", parameter.Name, err)
		}
		if value, has, err = parameterLocator.Value(ctx, parameter.In.Name); err != nil {
			return nil, false, err
		}
	}
	return s.adjustAndCache(ctx, parameter, opts, has, value, cachable)
}

func (s *Session) adjustAndCache(ctx context.Context, parameter *state.Parameter, opts *Options, has bool, value interface{}, cachable bool) (interface{}, bool, error) {
	var err error
	if !has && parameter.Value != nil {
		has = true
		value = parameter.Value
	}
	if !has {
		return nil, has, nil
	}
	if value, err = s.adjustValue(parameter, value); err != nil {
		return nil, false, err
	}
	if parameter.Output != nil {
		transformed, err := parameter.Output.Transform(ctx, value, opts.codecOptions...)
		if err != nil {
			return nil, false, fmt.Errorf("failed to transform %s with %s: %v, %w", parameter.Name, parameter.Output.Name, value, err)
		}
		value = transformed
	}
	if has && err == nil && cachable {
		s.setValue(parameter, value)
	}
	return value, has, err
}

// SetValue sets value to session cache
func (s *Session) setValue(parameter *state.Parameter, value interface{}) {
	s.cache.put(parameter, value)
}

func (s *Session) adjustValue(parameter *state.Parameter, value interface{}) (interface{}, error) {
	var err error
	switch actual := value.(type) {
	case string:
		if textValue, ok := value.(string); ok {
			value, _, err = converter.Convert(textValue, parameter.Schema.Type(), false, parameter.DateFormat)
		}
	case []string:
		if rType := parameter.OutputType(); rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
			repeated := converter.Repeated(actual)
			if v, err := repeated.Convert(rType); v != nil || err != nil {
				return v, err
			}
		} else if len(actual) > 0 { //destination is not a slice, thus using the first element
			value = actual[0]
		}
	}
	return value, err
}

func (s *Options) apply(options []Option) {
	for _, opt := range options {
		opt(s)
	}
	if s.kindLocator == nil {
		s.kindLocator = locator.NewKindsLocator(nil, s.locatorOptions...)
	}
	if s.state == nil {
		s.state = view.NewState()
	}
}

func New(aView *view.View, opts ...Option) *Session {
	ret := &Session{
		Options: Options{namespacedView: *view.IndexViews(aView)},
		cache:   newCache(),
		views:   newViews(),
		Types:   *state.NewTypes(),
	}
	ret.namedParameters = ret.namespacedView.Parameters()
	ret.apply(opts)
	return ret
}

func (s *Session) LoadState(parameters state.Parameters, aState interface{}) error {
	rType := reflect.TypeOf(aState)
	sType := structology.NewStateType(rType, structology.WithCustomizedNames(func(name string, tag reflect.StructTag) []string {
		stateTag, _ := tags.ParseStateTags(tag, nil)
		if stateTag == nil || stateTag.Parameter == nil || stateTag.Parameter.Name == "" {
			return []string{name}
		}
		return []string{stateTag.Parameter.Name}
	}))
	inputState := sType.WithValue(aState)
	ptr := xunsafe.AsPointer(aState)
	for _, parameter := range parameters {
		selector, _ := inputState.Selector(parameter.Name)
		if selector == nil {
			continue
		}
		if !selector.Has(ptr) {
			continue
		}
		value := selector.Value(ptr)
		switch parameter.In.Kind {
		case state.KindView, state.KindParam, state.KindState:
			if value == nil {
				return nil
			}

			rType := parameter.OutputType()
			if rType.Kind() == reflect.Ptr {
				ptr := (*unsafe.Pointer)(xunsafe.AsPointer(value))
				if ptr == nil || *ptr == nil {
					return nil
				}
			}
		}
		s.setValue(parameter, value)
	}

	return nil
}

func (s *Session) handleParameterError(parameter *state.Parameter, err error, errors *httputils.Errors) {
	if pErr, ok := err.(*httputils.Error); ok {
		pErr.StatusCode = parameter.ErrorStatusCode
		errors.Append(pErr)
	} else {
		errors.AddError("", parameter.Name, err, httputils.WithStatusCode(parameter.ErrorStatusCode))
	}
	if parameter.ErrorStatusCode != 0 {
		errors.SetStatus(parameter.ErrorStatusCode)
	} else if asErrors, ok := err.(*httputils.Errors); ok && asErrors.ErrorStatusCode() != http.StatusBadRequest {
		errors.SetStatus(asErrors.ErrorStatusCode())
	}
}

func (s *Session) InitKinds(kinds ...state.Kind) error {
	for _, kind := range kinds {
		if _, err := s.kindLocator.Lookup(kind); err != nil {
			return err
		}
	}
	return nil
}

// TODO deprecated this abstraction
type valueGetter struct {
	Parameters state.NamedParameters
	*Session
	*Options
}

func (g *valueGetter) Value(ctx context.Context, paramName string) (interface{}, error) {
	parameter, ok := g.Parameters[paramName]
	if !ok {
		return nil, fmt.Errorf("failed to lookup paramter: %v", paramName)
	}
	value, _, err := g.LookupValue(ctx, parameter, g.Options)
	return value, err
}
