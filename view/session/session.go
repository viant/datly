package session

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/structology"
)

type (
	Session struct {
		Selectors      *view.Selectors
		IndexedViews   *view.IndexedViews
		Locators       kind.Locators
		locatorOptions []locator.Option
	}
)

func (s *Session) Populate(ctx context.Context, aView *view.View) error {
	if err := s.populateTemplateParameters(ctx, aView); err != nil {
		return err
	}
	return nil
}

func (s *Session) populateTemplateParameters(ctx context.Context, aView *view.View) error {
	selectors := s.Selectors.Lookup(aView)
	if template := aView.Template; template != nil {
		stateType := template.State()
		parameters := template.Parameters
		if stateType.IsDefined() {
			aState := selectors.State
			err := s.populateParameters(ctx, parameters, aState)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Session) populateParameters(ctx context.Context, parameters state.Parameters, aState *structology.State) error {
	for _, parameter := range parameters {
		if err := s.populateParameter(ctx, parameter, aState); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) populateParameter(ctx context.Context, parameter *state.Parameter, aState *structology.State) error {
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
		parameterLocator, err := s.Locators.Lookup(parameter.In.Kind)
		if err != nil {
			return nil, false, fmt.Errorf("failed to locate parameter: %v, %w", parameter.Name, err)
		}
		if value, has, err = parameterLocator.Value(parameter.In.Name); err != nil {
			return nil, false, fmt.Errorf("failed to get  parameter value: %v, %w", parameter.Name, err)
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

func (s *Session) init(options []Option) {
	for _, opt := range options {
		opt(s)
	}
	if s.Locators == nil {
		s.Locators = locator.NewLocators(nil, s.locatorOptions...)
	}
}

func New(aView *view.View, opts ...Option) *Session {
	ret := &Session{IndexedViews: view.IndexViews(aView)}
	ret.init(opts)
	return ret
}
