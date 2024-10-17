package session

import (
	"context"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	hstate "github.com/viant/xdatly/handler/state"
	"reflect"
)

func (s *Session) ValuesOf(ctx context.Context, any interface{}) (map[string]interface{}, error) {
	anyType := reflect.TypeOf(any)
	if anyType.Kind() == reflect.Ptr {
		anyType = anyType.Elem()
	}
	aSchema := state.NewSchema(anyType)
	aType, err := state.NewType(state.WithSchema(aSchema))
	if err != nil {
		return nil, err
	}
	if err = aType.Init(); err != nil {
		return nil, err
	}
	var result = make(map[string]interface{})
	for _, parameter := range aType.Parameters {
		result[parameter.Name], _, err = s.LookupValue(ctx, parameter, nil)
	}
	return result, nil
}

func (s *Session) Into(ctx context.Context, dest interface{}, opts ...hstate.Option) (err error) {
	destType := reflect.TypeOf(dest)
	stateType, ok := s.Types.Lookup(types.EnsureStruct(destType))
	if !ok {
		if stateType, err = state.NewType(
			state.WithSchema(state.NewSchema(destType)),
			state.WithResource(s.resource),
		); err != nil {
			return err
		}
		s.Types.Put(stateType)
	}
	if destType.Kind() == reflect.Ptr {
		destType = destType.Elem()
	}

	hOptions := hstate.NewOptions(opts...)
	aState := stateType.Type().WithValue(dest)
	var stateOptions []locator.Option

	var locatorsToRemove = []state.Kind{state.KindComponent}
	if hOptions.Constants() != nil {
		stateOptions = append(stateOptions, locator.WithConstants(hOptions.Constants()))
		s.locatorOptions = append(s.locatorOptions, locator.WithConstants(hOptions.Constants()))
		locatorsToRemove = append(locatorsToRemove, state.KindConst)
	}

	var httpKinds = []state.Kind{state.KindForm, state.KindRequest, state.KindRequestBody, state.KindQuery, state.KindHeader, state.KindPath}

	//state.KindForm, state.KindRequest, state.KindQuery
	if hOptions.Form() != nil {
		stateOptions = append(stateOptions, locator.WithForm(hOptions.Form()))
		s.locatorOptions = append(s.locatorOptions, locator.WithForm(hOptions.Form()))
		locatorsToRemove = append(locatorsToRemove, httpKinds...)
	}
	if hOptions.Headers() != nil {
		stateOptions = append(stateOptions, locator.WithHeaders(hOptions.Headers()))
		s.locatorOptions = append(s.locatorOptions, locator.WithHeaders(hOptions.Headers()))
		locatorsToRemove = append(locatorsToRemove, httpKinds...)
	}
	if hOptions.Query() != nil {
		stateOptions = append(stateOptions, locator.WithQuery(hOptions.Query()))
		locatorsToRemove = append(locatorsToRemove, httpKinds...)
	}
	if len(hOptions.PathParameters()) > 0 {
		stateOptions = append(stateOptions, locator.WithPathParameters(hOptions.PathParameters()))
		locatorsToRemove = append(locatorsToRemove, httpKinds...)
	}
	if hOptions.HttpRequest() != nil {
		stateOptions = append(stateOptions, locator.WithRequest(hOptions.HttpRequest()))
		locatorsToRemove = append(locatorsToRemove, httpKinds...)
	}
	s.kindLocator.RemoveLocators(locatorsToRemove...)
	if s.view != nil {
		viewOptions := s.ViewOptions(s.view, WithLocatorOptions())
		stateOptions = append(viewOptions.kindLocator.Options(), stateOptions...)
	}

	if s.component != nil && s.component.Contract.Output.Type.Type().Type() == destType {
		return s.handleComponentpOutputType(ctx, dest, stateOptions)
	}

	options := s.Indirect(true, stateOptions...)
	options.scope = hOptions.Scope()
	if err = s.SetState(ctx, stateType.Parameters, aState, options); err != nil {
		return err
	}
	if initializer, ok := dest.(state.Initializer); ok {
		err = initializer.Init(ctx)
	}
	return err
}

func (s *Session) handleComponentpOutputType(ctx context.Context, dest interface{}, stateOptions []locator.Option) error {
	sessionOpt := s.Options
	s.Options = *s.Indirect(true, stateOptions...)
	destValue, err := s.operate(ctx, s, s.component)
	s.Options = sessionOpt
	if err != nil {
		return err
	}
	reflect.ValueOf(dest).Elem().Set(reflect.ValueOf(destValue).Elem())
	return nil
}
