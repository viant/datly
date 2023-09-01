package session

// Option represen session option
type Option func(s *Session)

type options []Option

func (o options) Apply(session *Session) {
	for _, opt := range o {
		opt(session)
	}
}
