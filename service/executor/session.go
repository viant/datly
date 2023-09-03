package executor

import (
	"fmt"
	expand "github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/service/executor/extension"
	vsession "github.com/viant/datly/service/session"
	"github.com/viant/datly/view"
	"github.com/viant/structology"
	"sync"
)

type Session struct {
	Session        *vsession.Session
	SessionHandler *extension.Session
	View           *view.View
	TemplateState  *expand.State
	DataUnit       *expand.DataUnit

	mux sync.Mutex
}

func NewSession(sessionState *vsession.Session, aView *view.View) (*Session, error) {
	return NewSessionWithCustomHandler(sessionState, aView, nil)
}

func NewSessionWithCustomHandler(state *vsession.Session, aView *view.View, handler *extension.Session) (*Session, error) {
	if aView == nil {
		return nil, fmt.Errorf("view was empty")
	}
	return &Session{
		Session:        state,
		View:           aView,
		SessionHandler: handler,
		mux:            sync.Mutex{},
	}, nil
}

// NewStatelet creates an executor state, TODO clerify why not use view resource state  (query selector can be done as *) ?

func (s *Session) Lookup(aView *view.View) *structology.State {
	aState := s.Session.State().Lookup(aView)
	return aState.Template
}
