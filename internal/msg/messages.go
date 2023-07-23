package msg

type Messages struct {
	Items []*Message
}

func (m *Messages) AddWarning(source, kind, message string) {
	msg := &Message{
		Level:   "warn",
		Source:  source,
		Kind:    kind,
		Message: message,
	}
	m.Items = append(m.Items, msg)
}
