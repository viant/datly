package expand

import (
	"context"

	"github.com/viant/datly/service/executor/extension"

	"github.com/viant/datly/view/state/predicate"
	"github.com/viant/structology"
	"github.com/viant/velty/est"
	"github.com/viant/xdatly/handler/mbus"
	"github.com/viant/xdatly/handler/validator"
)

type (
	State struct {
		*est.State
		*Context
		ParametersState  *structology.State
		EmbededVariables []*Variable
		NamedVariables   []*NamedVariable

		Expanded string
		flushed  bool
	}

	Context struct {
		Printer           *Printer           `velty:"names=logger|fmt"`
		DataUnit          *DataUnit          `velty:"names=sql|sqlx|sequencer|criteria"`
		Http              *Http              `velty:"names=http"`
		ResponseBuilder   *ResponseBuilder   `velty:"names=response"`
		ViewContext       *ViewContext       `velty:"names=View"`
		ParentViewContext *ViewContext       `velty:"names=ParentView"`
		Session           *extension.Session `velty:"names=session"`
		Validator         *validator.Service `velty:"names=validator"`
		MessageBus        *mbus.Service      `velty:"names=messageBus"`
		Predicate         *Predicate         `velty:"names=predicate"`
		Filters           predicate.Filters  `velty:"names=filters"`
		Context           context.Context    `velty:"-"`
	}

	StateOption func(state *State)
)

func WithViewParam(viewParam *ViewContext) StateOption {
	return func(state *State) {
		state.ViewContext = viewParam
	}
}

func WithNamedVariables(variables ...*NamedVariable) StateOption {
	return func(state *State) {
		state.NamedVariables = append(state.NamedVariables, variables...)
	}
}

func WithParentViewParam(viewParam *ViewContext) StateOption {
	return func(state *State) {
		state.ParentViewContext = viewParam
	}
}

func WithSession(session *extension.Session) StateOption {
	return func(state *State) {
		state.Session = session
	}
}

func WithParameterState(parametersState *structology.State) StateOption {
	return func(state *State) {
		state.ParametersState = parametersState
	}
}

func WithDataUnit(dataUnit *DataUnit) StateOption {
	return func(state *State) {
		state.DataUnit = dataUnit
	}
}

func WithCustomContext(customContext *Variable) StateOption {
	return func(state *State) {
		state.EmbededVariables = append(state.EmbededVariables, customContext)
	}
}

func (s *State) Init(templateState *est.State, predicates []*PredicateConfig, stateType *structology.StateType, options ...StateOption) {
	for _, option := range options {
		option(s)
	}

	if s.Printer == nil {
		s.Printer = &Printer{}
	}

	if s.Session == nil {
		s.Session = extension.NewSession()
	}

	if s.DataUnit == nil && s.ViewContext != nil {
		s.DataUnit = s.ViewContext.DataUnit
	}

	if s.DataUnit == nil {
		s.DataUnit = NewDataUnit(nil)
	}

	if s.Http == nil {
		s.Http = &Http{}
	}

	if s.ResponseBuilder == nil {
		s.ResponseBuilder = &ResponseBuilder{Content: map[string]interface{}{}}
	}

	if s.Validator == nil {
		s.Validator = s.Session.Validator()
	}

	if s.MessageBus == nil {
		s.MessageBus = s.Session.MessageBus()
	}

	s.Predicate = NewPredicate(s.Context, s.ParametersState, predicates, stateType)
	s.State = templateState
}

func (s *State) Flush(status HTTPStatus) error {
	if s.flushed {
		return nil
	}
	s.flushed = true

	if status == StatusSuccess {
		s.Printer.Flush()
	}

	if err := s.Http.Flush(status); err != nil {
		return err
	}

	return nil
}

func StateWithSQL(ctx context.Context, SQL string) *State {
	aState := &State{
		Expanded: SQL,
		Context:  &Context{Context: ctx},
	}

	aState.Init(nil, nil, nil)
	return aState
}
