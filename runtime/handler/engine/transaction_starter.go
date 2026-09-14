package engine

import (
	"context"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/runtime/handler/provider"
	xhandler "github.com/viant/xdatly/handler"
)

// Keep the concrete Data implementation behind the focused capability so
// handlers cannot type-assert their way into transaction completion methods.
type transactionStarter struct{ service xhandler.TransactionStarter }

func (s transactionStarter) Start(ctx context.Context) error { return s.service.Start(ctx) }

func (s *dataScope) transactionStarterProvider() locator.Provider {
	return provider.New(xhandler.TransactionStarterKey, func(ctx context.Context) (any, bool, error) {
		data, err := s.resolve(ctx)
		if err != nil || data == nil {
			return nil, false, err
		}
		starter, ok := data.(xhandler.TransactionStarter)
		if !ok {
			return nil, false, nil
		}
		return transactionStarter{service: starter}, true, nil
	})
}
