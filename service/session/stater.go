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
	viewOptions := s.ViewOptions(s.view, WithLocatorOptions())
	hOptions := hstate.NewOptions(opts...)
	aState := stateType.Type().WithValue(dest)
	stateOptions := viewOptions.kindLocator.Options()
	if hOptions.Constants() != nil {
		stateOptions = append(stateOptions, locator.WithConstants(hOptions.Constants()))
	}
	if hOptions.Form() != nil {
		stateOptions = append(stateOptions, locator.WithForm(hOptions.Form()))
	}
	if hOptions.HttpRequest() != nil {
		stateOptions = append(stateOptions, locator.WithRequest(hOptions.HttpRequest()))
	}
	options := s.Clone().Indirect(true, stateOptions...)
	options.scope = hOptions.Scope()
	if hOptions.HttpRequest() != nil || hOptions.Form() != nil {
		options.kindLocator.RemoveLocators(state.KindForm, state.KindRequest, state.KindQuery)
	}
	if hOptions.Constants() != nil {
		options.kindLocator.RemoveLocators(state.KindConst)
	}
	if err = s.SetState(ctx, stateType.Parameters, aState, options); err != nil {
		return err
	}
	if initializer, ok := dest.(state.Initializer); ok {
		err = initializer.Init(ctx)
	}
	return err
}
