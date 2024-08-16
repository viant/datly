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
	if hOptions.Constants() != nil {
		s.locatorOptions = append(s.locatorOptions, locator.WithConstants(hOptions.Constants()))
		s.kindLocator.RemoveLocators(state.KindConst)
		s.kindLocator.RemoveLocators(state.KindComponent)
	}
	if hOptions.Form() != nil {
		s.locatorOptions = append(s.locatorOptions, locator.WithForm(hOptions.Form()))
		s.kindLocator.RemoveLocators(state.KindForm)
		s.kindLocator.RemoveLocators(state.KindComponent)
	}
	if hOptions.HttpRequest() != nil {
		s.locatorOptions = append(s.locatorOptions, locator.WithRequest(hOptions.HttpRequest()))
		s.kindLocator.RemoveLocators(state.KindForm, state.KindRequest, state.KindQuery)
		s.kindLocator.RemoveLocators(state.KindComponent)
	}
	var stateOptions []locator.Option
	if s.view != nil {
		viewOptions := s.ViewOptions(s.view, WithLocatorOptions())
		stateOptions = viewOptions.kindLocator.Options()
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
