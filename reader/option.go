package reader

import (
	"github.com/viant/datly/view"
	"strings"
)

//SessionOption represents a session option
type SessionOption func(session *Session, aView *view.View) error

//WithCriteria returns criteria option
func WithCriteria(whereClause string, parameters ...interface{}) SessionOption {
	return func(session *Session, aView *view.View) error {
		session.AddCriteria(aView, whereClause, parameters...)
		return nil
	}
}

//WithParameter returns set parameter option
func WithParameter(name string, value interface{}) SessionOption {
	return func(session *Session, aView *view.View) error {
		paramName := name
		if !strings.Contains(paramName, ":") {
			paramName = aView.Name + ":" + name
		}
		return aView.SetParameter(paramName, session.Selectors, value)

	}
}
