package session

import (
	"context"
	"fmt"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind"
	"github.com/viant/datly/view/state/kind/locator"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
)

type (
	State struct {
		cache          *cache
		Selectors      *view.Selectors
		NamespacedView *view.NamespacedView
		Locators       *locator.Locators
		Parameters     state.NamedParameters
		locatorOptions []locator.Option //resousrce, route level options
	}

	LookupValueOptions struct {
		locators     kind.Locators
		codecOptions []codec.Options
	}

	LookupValueOption func(o *LookupValueOptions)
)

func (s *State) Populate(ctx context.Context, aView *view.View) error {
	if err := s.setTemplateState(ctx, aView); err != nil {
		return err
	}
	return nil
}

func (s *State) viewLookupOptions(aView *view.View) []locator.Option {
	var result []locator.Option
	result = append(result, locator.WithParameterLookup(s.LookupValue))
	result = append(result, locator.WithParameters(aView.Template.Parameters.Index()))
	return result
}

func (s *State) setTemplateState(ctx context.Context, aView *view.View) error {
	selectors := s.Selectors.Lookup(aView)
	if template := aView.Template; template != nil {
		stateType := template.State()
		parameters := template.Parameters
		locators := s.Locators.With(s.viewLookupOptions(aView)...)
		if stateType.IsDefined() {
			aState := selectors.State
			err := s.populateState(ctx, parameters, aState, locators)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *State) populateState(ctx context.Context, parameters state.Parameters, aState *structology.State, locators kind.Locators) error {
	if locators == nil {
		locators = s.Locators
	}
	for _, parameter := range parameters {
		if err := s.populateParameter(ctx, parameter, aState, locators); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) populateParameter(ctx context.Context, parameter *state.Parameter, aState *structology.State, locators kind.Locators) error {
	value, ok, err := s.LookupValue(ctx, parameter, locators)
	if !ok {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed read parameter: %v %w", parameter.Name, err)
	}
	return parameter.Selector().SetValue(aState.Pointer(), value)
}

func (s *State) lookupFirstValue(ctx context.Context, parameters []*state.Parameter) (value interface{}, has bool, err error) {
	for _, parameter := range parameters {
		value, has, err = s.LookupValue(ctx, parameter, nil)
		if has {
			return value, has, err
		}
	}
	return value, has, err
}

func (s *State) LookupValue(ctx context.Context, parameter *state.Parameter, locators kind.Locators) (value interface{}, has bool, err error) {
	if value, has = s.cache.lookup(parameter); has {
		return value, has, nil
	}

	switch parameter.In.Kind {
	case state.KindLiteral:
		value, has = parameter.Const, true
	default:
		parameterLocator, err := locators.Lookup(parameter.In.Kind)
		if err != nil {
			return nil, false, fmt.Errorf("failed to locate parameter: %v, %w", parameter.Name, err)
		}
		if value, has, err = parameterLocator.Value(ctx, parameter.In.Name); err != nil {
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
	if has && err == nil {
		s.cache.put(parameter, value)
	}
	return value, has, err
}

func (s *State) init(options []Option) {
	for _, opt := range options {
		opt(s)
	}
	if s.Locators == nil {
		s.Locators = locator.NewLocators(nil, s.locatorOptions...)
	}
	if s.Selectors == nil {
		s.Selectors = view.NewSelectors()
	}
}

func New(aView *view.View, opts ...Option) *State {
	ret := &State{NamespacedView: view.IndexViews(aView), cache: newCache()}
	ret.Parameters = ret.NamespacedView.Parameters()
	ret.init(opts)
	return ret
}
