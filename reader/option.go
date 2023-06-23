package reader

import (
	"github.com/viant/datly/view"
	"strings"
)

//Option represents a session option
type Option func(session *Session, aView *view.View) error

//Options represents option slice
type Options []Option

//apply applies options
func (o Options) Apply(session *Session, aView *view.View) error {
	if len(o) == 0 {
		return nil
	}
	for _, opt := range o {
		if err := opt(session, aView); err != nil {
			return err
		}
	}
	return nil
}

//WithCriteria returns criteria option
func WithCriteria(whereClause string, parameters ...interface{}) Option {
	return func(session *Session, aView *view.View) error {
		session.AddCriteria(aView, whereClause, parameters...)
		return nil
	}
}

//WithParameter returns set parameter option
func WithParameter(name string, value interface{}) Option {
	return func(session *Session, aView *view.View) error {
		paramName := name
		if !strings.Contains(paramName, ":") {
			paramName = aView.Name + ":" + name
		}
		return aView.SetParameter(paramName, session.Selectors, value)

	}
}
