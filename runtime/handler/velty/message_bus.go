package velty

import (
	"context"
	"fmt"

	xmbus "github.com/viant/xdatly/mbus"
)

type MessageBus struct {
	ctx     context.Context
	service xmbus.Service
}

func (b *MessageBus) Publish(destination string, value interface{}) (string, error) {
	if b == nil || b.service == nil {
		return "", fmt.Errorf("velty message bus capability is not configured")
	}
	confirmation, err := b.service.Push(b.ctx, b.service.Message(destination, value))
	if err != nil || confirmation == nil {
		return "", err
	}
	return confirmation.MessageID, nil
}

func (b *MessageBus) PublishWithSubject(destination string, value interface{}, subject string) (string, error) {
	if b == nil || b.service == nil {
		return "", fmt.Errorf("velty message bus capability is not configured")
	}
	confirmation, err := b.service.Push(b.ctx, b.service.Message(destination, value, xmbus.WithSubject(subject)))
	if err != nil || confirmation == nil {
		return "", err
	}
	return confirmation.MessageID, nil
}
