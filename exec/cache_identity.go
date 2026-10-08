package exec

import "context"

type cacheIdentityKey struct{}
type cacheIdentity struct {
	parameter string
	values    []any
}

// WithCacheIdentity scopes SQL-key rendering only. It grants no permission to
// execute rows or bypass ordinary authorization. Predicates may opt in after
// validating their sealed authority and every retained index value.
func WithCacheIdentity(ctx context.Context, parameter string, values []any) context.Context {
	return context.WithValue(ctx, cacheIdentityKey{}, cacheIdentity{parameter: parameter, values: append([]any(nil), values...)})
}
func CacheIdentityFromContext(ctx context.Context) (string, []any, bool) {
	if ctx == nil {
		return "", nil, false
	}
	identity, ok := ctx.Value(cacheIdentityKey{}).(cacheIdentity)
	if !ok || identity.parameter == "" || len(identity.values) == 0 {
		return "", nil, false
	}
	return identity.parameter, append([]any(nil), identity.values...), true
}
