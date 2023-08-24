package executor

import (
	"fmt"
	"github.com/viant/datly/executor/session"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	"github.com/viant/structology"
	"sync"
)

type Session struct {
	Parameters     *Parameters
	View           *view.View
	State          *expand.State
	SessionHandler *session.Session
	DataUnit       *expand.DataUnit

	mux       sync.Mutex
	selectors *view.ResourceState
}

func NewSession(selectors *view.ResourceState, aView *view.View) (*Session, error) {
	return NewSessionWithCustomHandler(selectors, aView, nil)
}

func NewSessionWithCustomHandler(selectors *view.ResourceState, aView *view.View, handler *session.Session) (*Session, error) {
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
	return &Parameters{index: map[string]*structology.State{}}
}

func (s *Session) Lookup(v *view.View) *structology.State {
	s.mux.Lock()
	state, ok := s.Parameters.index[v.Name]
	if !ok {
		state = v.Template.State().NewState()
		s.Parameters.index[v.Name] = state
	}
	s.mux.Unlock()
	return state
}

func (s *Session) Selectors() *view.ResourceState {
	return s.selectors
}
