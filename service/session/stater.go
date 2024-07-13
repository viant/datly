package session

import (
	"context"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
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
	sessionOptions := s.ViewOptions(s.view, WithLocatorOptions())
	hOptions := hstate.NewOptions(opts...)
	aState := stateType.Type().WithValue(dest)
	options := s.Clone().Indirect(true, sessionOptions.kindLocator.Options()...)
	options.scope = hOptions.Scope()
	if err = s.SetState(ctx, stateType.Parameters, aState, options); err != nil {
		return err
	}
	if initializer, ok := dest.(state.Initializer); ok {
		err = initializer.Init(ctx)
	}
	return err
}
