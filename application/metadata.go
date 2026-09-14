package application

import (
	"context"
	"github.com/viant/datly/spec"
)

type Metadata struct {
	Revision   uint64            `json:"revision"`
	Components []*spec.Component `json:"components"`
}

func (m *Manager) Metadata(ctx context.Context) (*Metadata, error) {
	_, current, err := m.pin(ctx)
	if err != nil {
		return nil, err
	}
	return &Metadata{Revision: current.revision, Components: current.runtime.Components()}, nil
}
