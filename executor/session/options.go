package session

import (
	"github.com/viant/datly/view"
)

//Option represen session option
type Option func(s *Session)

type options []Option

func (o options) Apply(session *Session) {
	for _, opt := range o {
		opt(session)
	}
}

func WithView(aView *view.View) Option {
	return func(session *Session) {
		session.view = aView
	}
}

func WithResource(resource *view.Resource) Option {
	return func(session *Session) {
		session.resource = resource
	}
}
