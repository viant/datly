package expand

import (
	"github.com/viant/datly/logger"
	"github.com/viant/velty/est"
)

type State struct {
	Expanded string
	*est.State
	Printer         *logger.Printer
	DataUnit        *DataUnit
	Http            *Http
	ResponseBuilder *ResponseBuilder
}

func (s *State) Init(templateState *est.State, param *MetaParam) {
	if s.Printer == nil {
		s.Printer = &logger.Printer{}
	}

	if param != nil && param.dataUnit != nil {
		s.DataUnit = param.dataUnit
	} else if s.DataUnit == nil {
		s.DataUnit = &DataUnit{}
	}

	if s.Http == nil {
		s.Http = &Http{}
	}

	if s.ResponseBuilder == nil {
		s.ResponseBuilder = &ResponseBuilder{content: map[string]interface{}{}}
	}

	s.State = templateState
}

func (s *State) Flush() error {
	s.Printer.Flush()

	return nil
}

func StateWithSQL(SQL string) *State {
	aState := &State{
		Expanded: SQL,
	}

	aState.Init(nil, nil)
	return aState
}
