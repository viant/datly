package session

import (
	"context"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view/state"
	"reflect"
)

func (s *State) Into(ctx context.Context, dest interface{}) (err error) {
	destType := reflect.TypeOf(dest)
	stateType, ok := s.Types.Lookup(types.EnsureStruct(destType))
	if !ok {
		if stateType, err = state.NewType(state.WithSchema(state.NewSchema(destType))); err != nil {
			return err
		}
		s.Types.Put(stateType)
	}

	aState := stateType.Type().WithValue(dest)
	options := s.Clone()
	err = s.SetState(ctx, stateType.Parameters, aState, options)
	return err
}
