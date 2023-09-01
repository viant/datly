package session

import (
	"context"
	"fmt"
	"github.com/viant/cloudless/async/mbus"
	xmbus "github.com/viant/xdatly/handler/mbus"
	"strings"
)

type MBus struct {
	MessageBuses map[string]*mbus.Resource
}

func (m *MBus) Push(ctx context.Context, msg *xmbus.Message) (*xmbus.Confirmation, error) {
	resource, err := m.Resource(msg.Resource)
	if err != nil {
		return nil, err
	}
	service := mbus.Lookup(resource.Vendor)
	if service == nil {
		return nil, fmt.Errorf("failed to lookup mbus vendor %s", resource.Vendor)
	}
	message := &mbus.Message{
		ID:         msg.ID,
		Resource:   resource,
		TraceID:    msg.TraceID,
		Attributes: msg.Attributes,
		Subject:    msg.Subject,
		Data:       msg.Data,
	}
	confirmation, err := service.Push(ctx, resource, message)
	if err != nil {
		return nil, err
	}
	return &xmbus.Confirmation{MessageID: confirmation.MessageID}, nil
}

func (m *MBus) Message(dest string, data interface{}, opts ...xmbus.Option) *xmbus.Message {
	ret := &xmbus.Message{Resource: dest, Data: data}
	xmbus.Options(opts).Apply(ret)
	return ret
}

func (m *MBus) Resource(dest string) (*mbus.Resource, error) {
	if isEncoded := isEncoded(dest); isEncoded {
		if resource, _ := mbus.EncodedResource(dest).Decode(); resource != nil {
			return resource, nil
		}
	}
	res, ok := m.MessageBuses[dest]
	if !ok {
		return nil, fmt.Errorf("failed to locate mbus for %v", dest)
	}
	return res, nil
}

func isEncoded(encoded string) bool {
	isEncoded := strings.Contains(encoded, "/")
	return isEncoded
}

func NewMBus(messageBusses map[string]*mbus.Resource) *xmbus.Service {
	if len(messageBusses) == 0 {
		messageBusses = map[string]*mbus.Resource{}
	}
	ret := &MBus{MessageBuses: messageBusses}
	return xmbus.New(ret)
}
