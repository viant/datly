package session

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/structology"
)

type Session struct {
	Selectors *view.Selectors
	MainView  *view.View
	Locators  kind.Locators
}

func (s *Session) Populate(ctx context.Context, aView *view.View) error {
	selectors := s.Selectors.Lookup(aView)
	if err := s.populateTemplateParameters(ctx, aView, selectors); err != nil {
		return err
	}
	return nil
}

func (s *Session) populateTemplateParameters(ctx context.Context, aView *view.View, selectors *view.Selector) error {
	if template := aView.Template; template != nil {
		aState := template.State()
		parameters := template.Parameters
		if aState.IsDefined() {
			for _, parameter := range parameters {
				if err := s.populateWithParameter(ctx, parameter, selectors.State); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Session) populateWithParameter(ctx context.Context, parameter *state.Parameter, aState *structology.State) error {
	value, ok, err := s.parameterValue(ctx, parameter, aState)
	if !ok {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed read parameter: %v %w", parameter.Name, err)
	}
	return parameter.Selector().SetValue(aState.Pointer(), value)
}

func (s *Session) parameterValue(ctx context.Context, parameter *state.Parameter, aState *structology.State) (value interface{}, has bool, err error) {
	switch parameter.In.Kind {
	case state.KindLiteral:
		value, has = parameter.Const, true
	default:
		locator := s.Locators.Lookup(parameter.In.Kind)
		if value, has, err = locator.Value(parameter.In.Name); err != nil {
			return nil, false, fmt.Errorf("faiedl to locate parameter: %v, %w", parameter.Name, err)
		}
	}
	if parameter.Output != nil {
		transformed, err := parameter.Output.Transform(ctx, value)
		if err != nil {
			return nil, false, fmt.Errorf("failed to decode parameter %v, value: %v, %w", parameter.Name, value, err)
		}
		value = transformed
	}
	return value, has, err
}
