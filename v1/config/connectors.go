package config

import (
	"context"
	"fmt"
)

type Connectors map[string]*Connector

func (v *Connectors) Register(connector *Connector) {
	if len(*v) == 0 {
		*v = make(map[string]*Connector)
	}
	(*v)[connector.Name] = connector
}

func (v Connectors) Lookup(ref string) (*Connector, error) {
	if len(v) == 0 {
		return nil, fmt.Errorf("failed to lookup connector %v", ref)
	}
	ret, ok := v[ref]
	if !ok {
		return nil, fmt.Errorf("failed to lookup connector %v", ref)
	}
	return ret, nil
}

type ConnectorSlice []*Connector

func (c ConnectorSlice) Index() Connectors {
	result := Connectors(map[string]*Connector{})

	for i := range c {
		result.Register(c[i])
	}
	return result
}

func (c ConnectorSlice) Init(ctx context.Context, connectors Connectors) error {
	for i := range c {
		if err := c[i].Init(ctx, connectors); err != nil {
			return err
		}
	}
	return nil
}
