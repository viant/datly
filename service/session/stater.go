package session

import (
	"context"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
	hstate "github.com/viant/xdatly/handler/state"
	"reflect"
)

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

	hOptions := hstate.NewOptions(opts...)
	aState := stateType.Type().WithValue(dest)
	var stateOptions []locator.Option

	var locatorsToRemove = []state.Kind{state.KindComponent}
	if hOptions.Constants() != nil {
		stateOptions = append(stateOptions, locator.WithConstants(hOptions.Constants()))
		s.locatorOptions = append(s.locatorOptions, locator.WithConstants(hOptions.Constants()))
		locatorsToRemove = append(locatorsToRemove, state.KindConst)
	}
	if hOptions.Form() != nil {
		stateOptions = append(stateOptions, locator.WithForm(hOptions.Form()))
		s.locatorOptions = append(s.locatorOptions, locator.WithForm(hOptions.Form()))
		locatorsToRemove = append(locatorsToRemove, state.KindConst, state.KindForm, state.KindComponent)
	}
	if len(hOptions.PathParameters()) > 0 {
		stateOptions = append(stateOptions, locator.WithPathParameters(hOptions.PathParameters()))
		s.locatorOptions = append(s.locatorOptions, locator.WithPathParameters(hOptions.PathParameters()))
		locatorsToRemove = append(locatorsToRemove, state.KindPath)
	}
	if hOptions.HttpRequest() != nil {
		stateOptions = append(stateOptions, locator.WithRequest(hOptions.HttpRequest()))
		s.locatorOptions = append(s.locatorOptions, locator.WithRequest(hOptions.HttpRequest()))
		locatorsToRemove = append(locatorsToRemove, state.KindForm, state.KindRequest, state.KindQuery)
	}

	s.kindLocator.RemoveLocators(locatorsToRemove...)
	if s.view != nil {
		viewOptions := s.ViewOptions(s.view, WithLocatorOptions())
		stateOptions = append(viewOptions.kindLocator.Options(), stateOptions...)
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
