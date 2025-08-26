package session

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"runtime/debug"

	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/xdatly/handler/response"
	hstate "github.com/viant/xdatly/handler/state"
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

// Into binds state into provided destination
func (s *Session) Into(ctx context.Context, dest interface{}, opts ...hstate.Option) (err error) {
	return s.Bind(ctx, dest, opts...)
}

func (s *Session) Bind(ctx context.Context, dest interface{}, opts ...hstate.Option) (err error) {
	defer func() {
		if r := recover(); r != nil {
			panicMsg := fmt.Sprintf("Panic occurred: %v, Stack trace: %v", r, string(debug.Stack()))
			logger := s.Logger()
			if logger == nil {
				panic(panicMsg)
			}
			s.Logger().Errorc(ctx, panicMsg)
			err = response.NewError(http.StatusInternalServerError, "Internal server error")
		}
	}()

	destType := reflect.TypeOf(dest)
	sType := types.EnsureStruct(destType)
	stateType, ok := s.Types.Lookup(sType)
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
	var stateOptions = []locator.Option{
		locator.WithLogger(s.logger),
	}
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

	reflectDestValue := reflect.ValueOf(destValue)
	if reflectDestValue.Kind() == reflect.Ptr {
		reflect.ValueOf(dest).Elem().Set(reflectDestValue.Elem())
	} else {
		reflect.ValueOf(dest).Elem().Set(reflectDestValue)
	}
	if err != nil {
		return err
	}
	return nil
}
