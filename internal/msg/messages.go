package msg

import "fmt"

type Messages struct {
	Items []*Message
}

func (m *Messages) AddWarning(source, kind, message string) {
	fmt.Printf("[WARN] %v\n", message)
	msg := &Message{
		Level:   "warn",
		Source:  source,
		Kind:    kind,
		Message: message,
	}
	m.Items = append(m.Items, msg)
}
