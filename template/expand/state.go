package expand

import (
	"github.com/viant/velty/est"
)

type State struct {
	*est.State
	Expanded        string
	Printer         *Printer
	DataUnit        *DataUnit
	Http            *Http
	ResponseBuilder *ResponseBuilder
	flushed         bool
}

func (s *State) Init(templateState *est.State, param *MetaParam) {
	if s.Printer == nil {
		s.Printer = &Printer{}
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
		s.ResponseBuilder = &ResponseBuilder{Content: map[string]interface{}{}}
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
