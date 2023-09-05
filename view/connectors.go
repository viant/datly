package view

import (
	"context"
	"fmt"
)

// Connectors represents Connector registry
// Output was produced based on Connector.Name
type Connectors map[string]*Connector

// Register registers connector
func (v *Connectors) Register(connector *Connector) {
	if len(*v) == 0 {
		*v = make(map[string]*Connector)
	}
	(*v)[connector.Name] = connector
}

// Lookup returns Connector by Connector.Name
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

// ConnectorSlice represents Repeated of *Connector
type ConnectorSlice []*Connector

// Views indexes Connectors by Connector.Name.
func (c ConnectorSlice) Index() Connectors {
	result := Connectors(map[string]*Connector{})
	c.IndexInto(&result)
	return result
}

func (c ConnectorSlice) IndexInto(result *Connectors) {
	for i := range c {
		result.Register(c[i])
	}
}

// Init initializes each connector
func (c ConnectorSlice) Init(ctx context.Context, connectors Connectors) error {
	for i := range c {
		if err := c[i].Init(ctx, connectors); err != nil {
			return err
		}
	}
	return nil
}
