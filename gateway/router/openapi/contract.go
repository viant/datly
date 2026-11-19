package openapi

import (
	"context"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
)

type contractResolverKey struct{}
type ContractResolver func(context.Context, *repository.Component, contract.Path) (*repository.EffectiveContract, error)

// WithContractResolver supplies the native request binding scope to documentation
// generation. It is request-local; no caller metadata mutates registered views.
func WithContractResolver(ctx context.Context, resolver ContractResolver) context.Context {
	return context.WithValue(ctx, contractResolverKey{}, resolver)
}
