package reader

import (
	"strings"

	"github.com/viant/datly/view"
	"github.com/viant/sqlx/io/read/cache"
)

// Option represents a session option
type Option func(session *Session) error

// Options represents option slice
type options []Option

// Apply sets options
func (o options) Apply(session *Session) error {
	if len(o) == 0 {
		return nil
	}
	for _, opt := range o {
		if err := opt(session); err != nil {
			return err
		}
	}
	return nil
}

// WithResourceState returns states option
func WithResourceState(states *view.State) Option {
	return func(session *Session) error {
		session.State = states
		return nil
	}
}

// WithParent returns parent option
func WithParent(parent *view.View) Option {
	return func(session *Session) error {
		session.Parent = parent
		return nil
	}
}

// WithCriteria returns criteria option
func WithCriteria(whereClause string, parameters ...interface{}) Option {
	return func(session *Session) error {
		aView := session.View
		session.AddCriteria(aView, whereClause, parameters...)
		return nil
	}
}

// WithQuery supplies an already parameterized query to the regular view
// reader. The reader still owns connector access, row collection, codecs,
// metrics, and error handling; only SQL construction is skipped.
func WithQuery(SQL string, args ...interface{}) Option {
	return func(session *Session) error {
		session.Query = &cache.ParmetrizedQuery{
			SQL:  SQL,
			Args: append([]interface{}{}, args...),
		}
		return nil
	}
}

// WithParameter returns set parameter option
func WithParameter(name string, value interface{}) Option {
	return func(session *Session) error {
		aView := session.View
		paramName := name
		if !strings.Contains(paramName, ":") {
			paramName = aView.Name + ":" + name
		}
		return aView.SetParameter(paramName, session.State, value)
	}
}

// WithCacheDisabled return
func WithCacheDisabled(flag bool) Option {
	return func(session *Session) error {
		session.CacheDisabled = flag
		return nil
	}
}

func WithRevealMetric(flag bool) Option {
	return func(session *Session) error {
		session.RevealMetric = flag
		return nil
	}
}

func WithCacheRefresh() Option {
	return func(session *Session) error {
		session.CacheRefresh = true
		return nil
	}
}
