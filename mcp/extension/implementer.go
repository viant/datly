package extension

import (
	"context"
	"github.com/viant/jsonrpc/transport"
	"github.com/viant/mcp-protocol/client"
	"github.com/viant/mcp-protocol/logger"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp-protocol/server"
)

type (
	Implementer struct {
		*server.DefaultImplementer
	}
)

// Implements checks if the method is implemented
func (i *Implementer) Implements(method string) bool {
	switch method {
	case schema.MethodResourcesList,
		schema.MethodResourcesTemplatesList,
		schema.MethodResourcesRead,
		schema.MethodSubscribe,
		schema.MethodUnsubscribe,
		schema.MethodToolsList,
		schema.MethodToolsCall:
		return true
	}
	return false
}

// New creates a new implementer
func New(registry *server.Registry) server.NewImplementer {
	return func(_ context.Context, notifier transport.Notifier, logger logger.Logger, client client.Operations) (server.Implementer, error) {
		base := server.NewDefaultImplementer(notifier, logger, client)
		base.Registry = registry
		return &Implementer{
			DefaultImplementer: base,
		}, nil
	}
}
