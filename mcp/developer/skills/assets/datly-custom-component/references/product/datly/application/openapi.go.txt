package application

import (
	"context"
	"github.com/viant/datly/gateway/openapi"
)

// ExportOpenAPI exports the pinned generation's public document. This is a
// trusted application API; network document-access policy belongs to HTTP.
func (m *Manager) ExportOpenAPI(ctx context.Context, request openapi.ExportRequest) ([]byte, error) {
	_, current, err := m.pin(ctx)
	if err != nil {
		return nil, err
	}
	return current.http.ExportOpenAPI(request)
}
