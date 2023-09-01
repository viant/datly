package executor

import (
	"fmt"
	"github.com/viant/datly/service/executor/session"
	"github.com/viant/datly/template/expand"
	"github.com/viant/datly/view"
	vsession "github.com/viant/datly/view/session"
	"github.com/viant/structology"
	"sync"
)

type Session struct {
	SessionState   *vsession.State
	View           *view.View
	TemplateState  *expand.State
	SessionHandler *session.Session
	DataUnit       *expand.DataUnit

	mux sync.Mutex
}

func NewSession(sessionState *vsession.State, aView *view.View) (*Session, error) {
	return NewSessionWithCustomHandler(sessionState, aView, nil)
}

func NewSessionWithCustomHandler(state *vsession.State, aView *view.View, handler *session.Session) (*Session, error) {
	if aView == nil {
		return nil, fmt.Errorf("view was empty")
	}
	return &Session{
		SessionState:   state,
		View:           aView,
		SessionHandler: handler,
		mux:            sync.Mutex{},
	}, nil
}

// NewStatelet creates an executor state, TODO clerify why not use view resource state  (query selector can be done as *) ?

func (s *Session) Lookup(aView *view.View) *structology.State {
	aState := s.SessionState.State().Lookup(aView)
	return aState.Template
}
