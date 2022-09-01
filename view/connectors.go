package view

import (
	"context"
	"fmt"
	"github.com/viant/datly/shared"
)

//Connectors represents Connector registry
//Value was produced based on Connector.Name
type Connectors map[string]*Connector

//Register registers connector
func (v *Connectors) Register(connector *Connector) {
	if len(*v) == 0 {
		*v = make(map[string]*Connector)
	}

	keys := shared.KeysOf(connector.Name, false)
	for i := range keys {
		(*v)[keys[i]] = connector
	}
}

//Lookup returns Connector by Connector.Name
func (v Connectors) Lookup(name string) (*Connector, error) {
	if len(v) == 0 {
		return nil, fmt.Errorf("failed to lookup connector %v", name)
	}
	ret, ok := v[name]
	if !ok {
		return nil, fmt.Errorf("failed to lookup connector %v", name)
	}
	return ret, nil
}

//ConnectorSlice represents Slice of *Connector
type ConnectorSlice []*Connector

//Index indexes Connectors by Connector.Name.
func (c ConnectorSlice) Index() Connectors {
	result := Connectors(map[string]*Connector{})

	for i := range c {
		result.Register(c[i])
	}
	return result
}

//Init initializes each connector
func (c ConnectorSlice) Init(ctx context.Context, connectors Connectors) error {
	for i := range c {
		if err := c[i].Init(ctx, connectors); err != nil {
			return err
		}
	}
	return nil
}
