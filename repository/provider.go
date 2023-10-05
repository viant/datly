package repository

import (
	"context"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/version"
	"sync"
)

type Provider struct {
	mux          sync.RWMutex
	path         contract.Path
	control      *version.Control
	newComponent func(ctx context.Context, opts ...Option) (*Component, error)
	component    *Component
}

func (p *Provider) Component(ctx context.Context, opts ...Option) (*Component, error) {
	p.mux.RLock()
	ret := p.component
	inSync := ret != nil && ret.Version.SCN == p.control.SCN
	p.mux.RUnlock()
	if inSync {
		return ret, nil
	}
	p.mux.Lock()
	defer p.mux.Unlock()
	if p.control.ChangeKind() == version.ChangeKindDeleted {
		//TODO maybe return 404 error
		return nil, nil
	}
	aComponent, err := p.newComponent(ctx, opts...)
	if err != nil {
		return nil, err
	}
	aComponent.Version.SCN = p.control.SCN
	p.component = aComponent
	return aComponent, nil
}

func NewProvider(path contract.Path, control *version.Control, newComponent func(ctx context.Context, opts ...Option) (*Component, error)) *Provider {
	return &Provider{path: path, control: control, newComponent: newComponent}
}
