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
		Context
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
	}
)

func (s *State) Init(templateState *est.State, param *MetaParam, parent *MetaParam, validator *Validator, sess *session.Session) {
	if s.Printer == nil {
		s.Printer = &Printer{}
	}

	if sess == nil {
		sess = session.NewSession()
	}

	if s.Session == nil {
		s.Session = sess
	}

	if param != nil && param.dataUnit != nil {
		s.DataUnit = param.dataUnit
	} else if s.DataUnit == nil {
		s.DataUnit = &DataUnit{
			stmts: NewStmtHolder(),
		}
	}

	if s.Http == nil {
		s.Http = &Http{}
	}

	if s.ResponseBuilder == nil {
		s.ResponseBuilder = &ResponseBuilder{Content: map[string]interface{}{}}
	}

	if s.ViewParam == nil {
		s.ViewParam = param
	}

	if s.ParentParam == nil {
		s.ParentParam = parent
	}

	if s.Validator == nil {
		s.Validator = s.Session.Validator()
	}

	if s.MessageBus == nil {
		s.MessageBus = s.Session.MessageBus()
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

	aState.Init(nil, nil, nil, nil, nil)
	return aState
}
