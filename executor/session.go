package executor

import (
	"fmt"
	"github.com/viant/datly/executor/session"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	"sync"
)

type Session struct {
	Parameters     *Parameters
	View           *view.View
	State          *expand.State
	SessionHandler *session.Session
	DataUnit       *expand.DataUnit

	mux       sync.Mutex
	selectors *view.Selectors
}

func NewSession(selectors *view.Selectors, aView *view.View) (*Session, error) {
	return NewSessionWithCustomHandler(selectors, aView, nil)
}

func NewSessionWithCustomHandler(selectors *view.Selectors, aView *view.View, handler *session.Session) (*Session, error) {
	if aView == nil {
		return nil, fmt.Errorf("view was empty")
	}
	parameters := NewParameters()
	for viewName := range selectors.Index {
		parameters.Add(viewName, selectors.Index[viewName])
	}

	return &Session{
		Parameters:     parameters,
		selectors:      selectors,
		View:           aView,
		SessionHandler: handler,
		mux:            sync.Mutex{},
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

func (s *Session) Selectors() *view.Selectors {
	return s.selectors
}
