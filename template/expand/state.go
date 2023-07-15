package expand

import (
	"github.com/viant/datly/executor/session"
	"github.com/viant/velty/est"
	"github.com/viant/xdatly/handler/mbus"
	"github.com/viant/xdatly/handler/validator"
)

type (
	State struct {
		*est.State
		*Context
		Parameters    interface{}
		ParametersHas interface{}
		CustomContext []*CustomContext

		Expanded string
		flushed  bool
	}

	Context struct {
		Printer         *Printer           `velty:"names=logger|fmt"`
		DataUnit        *DataUnit          `velty:"names=sql|sqlx|sequencer|criteria"`
		Http            *Http              `velty:"names=http"`
		ResponseBuilder *ResponseBuilder   `velty:"names=response"`
		ViewParam       *MetaParam         `velty:"names=View"`
		ParentParam     *MetaParam         `velty:"names=ParentView"`
		Session         *session.Session   `velty:"names=session"`
		Validator       *validator.Service `velty:"names=validator"`
		MessageBus      *mbus.Service      `velty:"names=messageBus"`
		Predicate       *Predicate         `velty:"names=predicate"`
	}

	StateOption func(state *State)
)

func WithViewParam(viewParam *MetaParam) StateOption {
	return func(state *State) {
		state.ViewParam = viewParam
	}
}

func WithParentViewParam(viewParam *MetaParam) StateOption {
	return func(state *State) {
		state.ParentParam = viewParam
	}
}

func WithSession(session *session.Session) StateOption {
	return func(state *State) {
		state.Session = session
	}
}

func WithParameters(params, has interface{}) StateOption {
	return func(state *State) {
		state.Parameters = params
		state.ParametersHas = has
	}
}

func WithDataUnit(dataUnit *DataUnit) StateOption {
	return func(state *State) {
		state.DataUnit = dataUnit
	}
}

func WithCustomContext(customContext *CustomContext) StateOption {
	return func(state *State) {
		state.CustomContext = append(state.CustomContext, customContext)
	}
}

func (s *State) Init(templateState *est.State, predicates []*PredicateConfig, options ...StateOption) {
	for _, option := range options {
		option(s)
	}

	if s.Printer == nil {
		s.Printer = &Printer{}
	}

	if s.Session == nil {
		s.Session = session.NewSession()
	}

	if s.DataUnit == nil && s.ViewParam != nil {
		s.DataUnit = s.ViewParam.dataUnit
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

	if len(predicates) > 0 {
		s.Predicate = NewPredicate(s.Context, s.Parameters, s.ParametersHas, predicates)
	}

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

func StateWithSQL(SQL string) *State {
	aState := &State{
		Expanded: SQL,
	}

	aState.Init(nil, nil)
	return aState
}
