package session

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xunsafe"
	"reflect"
	"sync"
	"unsafe"
)

type (
	State struct {
		cache *cache
		Options
	}
)

func (s *State) Populate(ctx context.Context) error {
	if len(s.namespacedView.Views) == 0 {
		return nil
	}
	wg := sync.WaitGroup{}
	errors := httputils.NewErrors()
	for i := range s.namespacedView.Views {
		wg.Add(1)
		go s.setViewStateInBackground(ctx, s.namespacedView.Views[i].View, errors, &wg)
	}
	wg.Wait()
	if !errors.HasError() {
		return nil
	}
	return errors
}

func (s *State) setViewStateInBackground(ctx context.Context, aView *view.View, errors *httputils.Errors, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := s.SetViewState(ctx, aView); err != nil {
		errors.Append(err)
	}
}

func (s *State) SetViewState(ctx context.Context, aView *view.View) error {

	opts := s.viewOptions(aView)
	if aView.Mode == view.ModeQuery {
		ns := s.namespacedView.ByName(aView.Name)
		if ns == nil {
			ns = &view.NamespaceView{View: aView, Path: "", Namespaces: []string{aView.Selector.Namespace}}
		}
		if err := s.setQuerySelector(ctx, ns, opts); err != nil {
			return err
		}
	}

	if err := s.setTemplateState(ctx, aView, opts); err != nil {
		switch actual := err.(type) {
		case *httputils.Error:
			actual.View = aView.Name
		case *httputils.Errors:
			for _, item := range actual.Errors {
				item.View = aView.Name
			}
		}
		return err
	}
	return nil
}

func (s *State) viewLookupOptions(parameters state.NamedParameters, opts *Options) []locator.Option {
	var result []locator.Option
	result = append(result, locator.WithParameterLookup(func(ctx context.Context, parameter *state.Parameter) (interface{}, bool, error) {
		return s.LookupValue(ctx, parameter, opts)
	}))
	result = append(result, locator.WithParameters(parameters))
	return result
}

func (s *State) viewOptions(aView *view.View) *Options {
	selectors := s.states.Lookup(aView)

	viewOptions := s.Options.Clone()
	var parameters state.NamedParameters
	if aView.Template != nil {
		parameters = aView.Template.Parameters.Index()
	}
	viewOptions.kindLocator = s.kindLocator.With(s.viewLookupOptions(parameters, viewOptions)...)

	viewOptions.AddCodec(codec.WithSelector(codec.Selector(selectors)))
	viewOptions.AddCodec(codec.WithColumnsSource(aView.IndexedColumns()))

	getter := &valueGetter{Parameters: parameters, State: s, Options: viewOptions}

	//TODO replace  with locator.ParameterLookup  option
	viewOptions.AddCodec(codec.WithValueGetter(getter))
	viewOptions.AddCodec(codec.WithValueLookup(getter.Value))
	//TODO end

	return viewOptions
}

// TODO deprecated this abstraction
type valueGetter struct {
	Parameters state.NamedParameters
	*State
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

func (s *State) setTemplateState(ctx context.Context, aView *view.View, opts *Options) error {
	selectors := s.states.Lookup(aView)
	if template := aView.Template; template != nil {
		stateType := template.State()
		if stateType.IsDefined() {
			aState := selectors.State
			err := s.populateState(ctx, template.Parameters, aState, opts)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *State) populateState(ctx context.Context, parameters state.Parameters, aState *structology.State, opts *Options) error {
	errors := httputils.NewErrors()
	wg := sync.WaitGroup{}
	for i, _ := range parameters { //populate non data view parameters first
		wg.Add(1)
		go s.populateParameterInBackground(ctx, parameters[i], aState, opts, errors, &wg)
	}
	wg.Wait()

	return nil
}

func (s *State) populateParameterInBackground(ctx context.Context, parameter *state.Parameter, aState *structology.State, options *Options, errors *httputils.Errors, wg *sync.WaitGroup) {
	defer wg.Done()
	if err := s.populateParameter(ctx, parameter, aState, options); err != nil {
		if pErr, ok := err.(*httputils.Error); ok {
			pErr.StatusCode = parameter.ErrorStatusCode
			errors.Append(pErr)
		} else {
			errors.AddError("", parameter.Name, err, httputils.WithStatusCode(parameter.ErrorStatusCode))
		}
	}
}

func (s *State) populateParameter(ctx context.Context, parameter *state.Parameter, aState *structology.State, options *Options) error {
	value, ok, err := s.LookupValue(ctx, parameter, options)
	if !ok {
		if parameter.IsRequired() {
			return fmt.Errorf("parameter %v is required", parameter.Name)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if value, err = s.ensureValidValue(value, parameter); err != nil {
		return err
	}
	return parameter.Selector().SetValue(aState.Pointer(), value)
}

func (s *State) ensureValidValue(value interface{}, parameter *state.Parameter) (interface{}, error) {
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
			return nil, httputils.NewParamError("", parameter.Name, errors.New(errorMessage), httputils.WithObject(value))
		}
		switch parameter.OutputType().Kind() {
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
	if ptr := xunsafe.AsPointer(value); *(*unsafe.Pointer)(ptr) == nil {
		return true
	}
	return false
}

func (s *State) lookupFirstValue(ctx context.Context, parameters []*state.Parameter, opts *Options) (value interface{}, has bool, err error) {
	for _, parameter := range parameters {
		value, has, err = s.LookupValue(ctx, parameter, opts)
		if has {
			return value, has, err
		}
	}
	return value, has, err
}

func (s *State) LookupValue(ctx context.Context, parameter *state.Parameter, opts *Options) (value interface{}, has bool, err error) {
	if opts == nil {
		opts = &s.Options
	}
	if value, has = s.cache.lookup(parameter); has {
		return value, has, nil
	}

	lock := s.cache.lockParameter(parameter) //lockParameter is to ensure value for a parameter is computed only once
	lock.Lock()
	defer lock.Unlock()
	if value, has = s.cache.lookup(parameter); has {
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
			return nil, false, fmt.Errorf("failed to get  parameter value: %v, %w", parameter.Name, err)
		}
	}
	if parameter.Output != nil {
		transformed, err := parameter.Output.Transform(ctx, value, opts.codecOptions...)
		if err != nil {
			return nil, false, fmt.Errorf("failed to decode parameter %v, value: %v, %w", parameter.Name, value, err)
		}
		value = transformed
	}
	if has && err == nil {
		s.cache.put(parameter, value)
	}
	return value, has, err
}

func (s *Options) apply(options []Option) {
	for _, opt := range options {
		opt(s)
	}
	if s.kindLocator == nil {
		s.kindLocator = locator.NewKindsLocator(nil, s.locatorOptions...)
	}
	if s.states == nil {
		s.states = view.NewStates()
	}
}

func New(aView *view.View, opts ...Option) *State {
	ret := &State{
		Options: Options{namespacedView: *view.IndexViews(aView)},
		cache:   newCache(),
	}
	ret.namedParameters = ret.namespacedView.Parameters()
	ret.apply(opts)
	return ret
}
