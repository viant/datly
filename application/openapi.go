package application

import (
	"context"
	"github.com/viant/datly/gateway/openapi"
)

// ExportOpenAPI exports the pinned generation's public document. This is a
// trusted application API; network document-access policy belongs to HTTP.
func (m *Manager) ExportOpenAPI(ctx context.Context, request openapi.ExportRequest) ([]byte, error) {
	_, current, release, err := m.pinOwned(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	if err := current.ensureDocuments(ctx); err != nil {
		return nil, err
	}
	current.httpMu.Lock()
	handler := current.http
	current.httpMu.Unlock()
	return handler.ExportOpenAPI(request)
}
