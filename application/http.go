package application

import (
	"context"
	"fmt"
	"net/http"

	gateway "github.com/viant/datly/gateway/http"
)

func (m *Manager) newHTTPHandler(ctx context.Context, built *Build, input gateway.HandlerInput) (*gateway.Handler, error) {
	httpConfig := built.HTTP
	if httpConfig.Warmup != nil {
		policy := *httpConfig.Warmup
		if policy.Lifetime != nil {
			return nil, fmt.Errorf("application owns HTTP warmup lifetime")
		}
		policy.Lifetime = m.warmups
		httpConfig.Warmup = &policy
	}
	return httpConfig.Build(ctx, input)
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, current, err := m.pin(r.Context())
	if err != nil {
		http.Error(w, "application unavailable", http.StatusServiceUnavailable)
		return
	}
	current.http.ServeHTTP(w, r.WithContext(ctx))
}
