package application

import (
	"context"
	gateway "github.com/viant/datly/gateway/http"
)

type jobAdmission struct {
	manager    *Manager
	generation *asyncGeneration
}

func (m *Manager) jobAdmission(generation *asyncGeneration) gateway.AsyncAdmission {
	if generation == nil {
		return nil
	}
	return &jobAdmission{manager: m, generation: generation}
}
func (a *jobAdmission) Begin(ctx context.Context) (context.Context, gateway.AsyncService, func(), error) {
	ctx, current, release, err := a.manager.admitAsync(ctx, a.generation)
	if err != nil {
		return ctx, nil, nil, err
	}
	return ctx, current.service, release, nil
}
