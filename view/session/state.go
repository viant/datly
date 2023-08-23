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
		cache *cache
		Options
	}

	LookupValueOptions struct {
		locators     kind.KindLocator
		codecOptions []codec.Options
	}

	LookupValueOption func(o *LookupValueOptions)
)

func (s *State) Populate(ctx context.Context, aView *view.View) error {
	opts := s.viewOptions(aView)
	if aView.Mode == view.ModeQuery {
		ns := s.namespacedView.ByName(aView.Name)
		if err := s.setQuerySelector(ctx, ns, opts); err != nil {
			return err
		}
	}
	if err := s.setTemplateState(ctx, aView, opts); err != nil {
		return err
	}
	return nil
}

func (s *State) viewLookupOptions(parameters state.NamedParameters, opts *Options) []locator.Option {
	var result []locator.Option
	result = append(result, locator.WithParameterLookup(func(ctx context.Context, parameter *state.Parameter) (interface{}, bool, error) {
		return s.LookupValue(ctx, parameter, opts)
	}))
	result = append(result, locator.WithParameters(parameters))
	return result
}

func (s *State) viewOptions(aView *view.View) *Options {
	selectors := s.selectors.Lookup(aView)

	viewOptions := s.Options.Clone()
	var parameters state.NamedParameters
	if aView.Template != nil {
		parameters = aView.Template.Parameters.Index()
	}
	viewOptions.kindLocator = s.kindLocator.With(s.viewLookupOptions(parameters, viewOptions)...)

	viewOptions.AddCodec(codec.WithSelector(codec.Selector(selectors)))
	viewOptions.AddCodec(codec.WithColumnsSource(aView.IndexedColumns()))

	getter := &valueGetter{Parameters: parameters, State: s, Options: viewOptions}

	//TODO replace  with locator.ParameterLookup  option
	viewOptions.AddCodec(codec.WithValueGetter(getter))
	viewOptions.AddCodec(codec.WithValueLookup(getter.Value))
	//TODO end

	return viewOptions
}

// TODO deprecated this abstraction
type valueGetter struct {
	Parameters state.NamedParameters
	*State
	*Options
}

func (g *valueGetter) Value(ctx context.Context, paramName string) (interface{}, error) {
	parameter, ok := g.Parameters[paramName]
	if !ok {
		return nil, fmt.Errorf("failed to lookup paramter: %v", paramName)
	}
	value, _, err := g.LookupValue(ctx, parameter, g.Options)
	return value, err
}

func (s *State) setTemplateState(ctx context.Context, aView *view.View, opts *Options) error {
	selectors := s.selectors.Lookup(aView)
	if template := aView.Template; template != nil {
		stateType := template.State()
		if stateType.IsDefined() {
			aState := selectors.State
			err := s.populateState(ctx, template.Parameters, aState, opts)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *State) populateState(ctx context.Context, parameters state.Parameters, aState *structology.State, opts *Options) error {
	for _, parameter := range parameters {
		if err := s.populateParameter(ctx, parameter, aState, opts); err != nil {
			return err
		}
	}
	return nil
}

func (s *State) populateParameter(ctx context.Context, parameter *state.Parameter, aState *structology.State, options *Options) error {
	value, ok, err := s.LookupValue(ctx, parameter, options)
	if !ok {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed read parameter: %v %w", parameter.Name, err)
	}
	return parameter.Selector().SetValue(aState.Pointer(), value)
}

func (s *State) lookupFirstValue(ctx context.Context, parameters []*state.Parameter, opts *Options) (value interface{}, has bool, err error) {
	for _, parameter := range parameters {
		value, has, err = s.LookupValue(ctx, parameter, opts)
		if has {
			return value, has, err
		}
	}
	return value, has, err
}

func (s *State) LookupValue(ctx context.Context, parameter *state.Parameter, opts *Options) (value interface{}, has bool, err error) {
	if opts == nil {
		opts = &s.Options
	}
	if value, has = s.cache.lookup(parameter); has {
		return value, has, nil
	}
	locators := opts.kindLocator
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
		transformed, err := parameter.Output.Transform(ctx, value, opts.codecOptions...)
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

func (s *Options) apply(options []Option) {
	for _, opt := range options {
		opt(s)
	}
	if s.kindLocator == nil {
		s.kindLocator = locator.NewKindsLocator(nil, s.locatorOptions...)
	}
	if s.selectors == nil {
		s.selectors = view.NewSelectors()
	}
}

func New(aView *view.View, opts ...Option) *State {
	ret := &State{
		Options: Options{namespacedView: view.IndexViews(aView)},
		cache:   newCache(),
	}
	ret.parameters = ret.namespacedView.Parameters()
	ret.apply(opts)
	return ret
}
