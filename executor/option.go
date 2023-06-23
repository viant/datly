package executor

//Option represents executor session option
type Option func(session *Session) error

//Options represents options
type Options []Option

//apply applies session option
func (o Options) Apply(session *Session) error {
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

//WithParameter return parameter option
func WithParameter(name string, value interface{}) Option {
	return func(session *Session) error {
		viewName := session.View.Name
		err := session.View.SetParameter(name, session.selectors, value)
		session.Parameters.Add(session.View.Name, session.selectors.Index[viewName])
		return err
	}
}
