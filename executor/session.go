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
	Parameters     *State
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
	parameters := NewState()
	for viewName := range selectors.Views {
		parameters.Add(viewName, selectors.Views[viewName])
	}

	return &Session{
		Parameters:     parameters,
		selectors:      selectors,
		View:           aView,
		SessionHandler: handler,
		mux:            sync.Mutex{},
	}, nil
}

// NewState creates an executor state, TODO clerify why not use view resource state  (query selector can be done as *) ?
func NewState() *State {
	return &State{index: map[string]*structology.State{}}
}

func (s *Session) Lookup(v *view.View) *structology.State {
	s.mux.Lock()
	state, ok := s.Parameters.index[v.Name]
	if !ok {
		state = v.Template.StateType().NewState()
		s.Parameters.index[v.Name] = state
	}
	s.mux.Unlock()
	return state
}

func (s *Session) Selectors() *view.ResourceState {
	return s.selectors
}
