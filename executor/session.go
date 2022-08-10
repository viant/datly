package executor

import (
	"github.com/viant/datly/view"
	"sync"
)

type Session struct {
	Parameters *Parameters
	View       *view.View
	mux        sync.Mutex
}

func NewSession(selectors *view.Selectors, aView *view.View) (*Session, error) {
	parameters := NewParameters()
	for viewName := range selectors.Index {
		parameters.Add(viewName, selectors.Index[viewName])
	}

	return &Session{
		Parameters: parameters,
		View:       aView,
		mux:        sync.Mutex{},
	}, nil
}

func NewParameters() *Parameters {
	return &Parameters{index: map[string]*view.ParamState{}}
}

func (s *Session) Lookup(v *view.View) *view.ParamState {
	s.mux.Lock()
	state, ok := s.Parameters.index[v.Name]
	if !ok {
		state = &view.ParamState{}
		s.Parameters.index[v.Name] = state
	}
	s.mux.Unlock()

	state.Init(v)
	return state
}
