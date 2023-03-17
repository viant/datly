package router

import (
	"fmt"
	"github.com/viant/datly/executor"
	"github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"reflect"
	"strings"
)

type (
	BodySelector struct {
		Query      string
		StateValue string

		_bodyType reflect.Type
		_accessor *types.Accessor
	}
)

func (s *BodySelector) Init(aView *view.View) error {
	if s.StateValue == "" {
		return fmt.Errorf("param name was not specified")
	}

	stateType := aView.Template.StateType()
	accessors := types.NewAccessors(&types.VeltyNamer{})

	actualName := s.stateValue(aView)
	accessors.InitPath(stateType, actualName)
	accessor, err := accessors.AccessorByName(actualName)
	if err != nil {
		return err
	}

	s._accessor = accessor
	resultType := accessor.Type()
	s._bodyType = resultType

	return nil
}

func (s *BodySelector) stateValue(aView *view.View) string {
	if strings.HasPrefix(s.StateValue, "Unsafe") {
		return s.StateValue
	}

	if _, err := aView.ParamByName(s.StateValue); err == nil {
		return "Unsafe." + s.StateValue
	}

	return s.StateValue
}

func (s *BodySelector) getValue(session *executor.Session) (interface{}, error) {
	return s._accessor.Value(session.State.Mem)
}
