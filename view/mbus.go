package view

import (
	"fmt"
	"github.com/viant/cloudless/async/mbus"
)

type (

	//MessageBuses represents message bus resource

	MessageBusSlice []*mbus.Resource

	//MessageBuses message bus map
	MessageBuses map[string]*mbus.Resource
)

func (m MessageBusSlice) Index() MessageBuses {
	var result = MessageBuses{}
	if len(m) == 0 {
		return result
	}
	for i, item := range m {
		result[item.Name] = m[i]
	}
	return result
}

//Lookup returns message bus for supplied name or error
func (m MessageBuses) Lookup(name string) (*mbus.Resource, error) {
	ret, ok := m[name]
	if !ok {
		return nil, fmt.Errorf("failed to lookup message bus: %s", name)
	}
	return ret, nil
}
