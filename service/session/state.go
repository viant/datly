package session

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/datly/internal/converter"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xunsafe"
	"net/http"
	"reflect"
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
)

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
		ns := s.namespacedView.ByName(aView.Name)
		if ns == nil {
			ns = &view.NamespaceView{View: aView, Path: "", Namespaces: []string{aView.Selector.Namespace}}
		}
		if err = s.setQuerySelector(ctx, ns, opts); err != nil {
			return err
		}
	}
	if err = s.setTemplateState(ctx, aView, opts); err != nil {
		s.adjustErrorSource(err, aView)
	}
	return err
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

func (s *Session) ViewOptions(aView *view.View) *Options {
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

	return viewOptions
}

func (s *Session) setTemplateState(ctx context.Context, aView *view.View, opts *Options) error {
	state := s.state.Lookup(aView)
	if template := aView.Template; template != nil {
		stateType := template.StateType()
		if stateType.IsDefined() {
			aState := state.Template
			err := s.SetState(ctx, template.Parameters, aState, opts)
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
	if value, err = s.ensureValidValue(value, parameter); err != nil {
		return err
	}
	if options.indirectState { //p
		fmt.Printf("%T %+v %T\n", aState.State(), aState.State(), value) // arameters.Selectors are not initialized from the tempalte state
		err = aState.SetValue(parameter.Name, value)
		fmt.Printf("AFTER %T %+v %T %+v\n", aState.State(), aState.State(), value, value) // arameters.Selectors are not initialized from the tempalte state
		return err
	}
	err = parameter.Selector().SetValue(aState.Pointer(), value)
	return err
}

func (s *Session) ensureValidValue(value interface{}, parameter *state.Parameter) (interface{}, error) {
	valueType := reflect.TypeOf(value)
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
			case 1:
				value = slice.ValuePointerAt(ptr, 0)
			default:
				return nil, fmt.Errorf("parameter %v return more than one value, len: %v rows ", parameter.Name, sliceLen)
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

func (s *Session) lookupValue(ctx context.Context, parameter *state.Parameter, opts *Options) (value interface{}, has bool, err error) {
	if opts == nil {
		opts = &s.Options
	}
	cachable := parameter.In.Kind != state.KindState

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
	case state.KindLiteral:
		value, has = parameter.Const, true
	default:
		parameterLocator, err := locators.Lookup(parameter.In.Kind)
		if err != nil {
			return nil, false, fmt.Errorf("failed to locate parameter: %v, %w", parameter.Name, err)
		}
		if value, has, err = parameterLocator.Value(ctx, parameter.In.Name); err != nil {
			return nil, false, err
		}
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
			return nil, false, fmt.Errorf("failed to transform: %v, %w", value, err)
		}
		value = transformed
	}
	if has && err == nil && cachable {
		s.cache.put(parameter, value)
	}
	return value, has, err
}

func (s *Session) adjustValue(parameter *state.Parameter, value interface{}) (interface{}, error) {
	var err error
	if parameter.Schema.Type().Kind() != reflect.String { //TODO add support for all incompatible types
		if textValue, ok := value.(string); ok {
			value, _, err = converter.Convert(textValue, parameter.Schema.Type(), false, parameter.DateFormat)
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
	}
	ret.namedParameters = ret.namespacedView.Parameters()
	ret.apply(opts)
	return ret
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
