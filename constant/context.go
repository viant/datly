package constant

import "context"

type valuesKey struct{}

// WithContext attaches immutable component constants at an invocation boundary.
// A nil value shadows parent constants, just as an empty child component does.
func WithContext(ctx context.Context, values *Values) context.Context {
	return context.WithValue(ctx, valuesKey{}, values)
}

func FromContext(ctx context.Context) *Values {
	if ctx == nil {
		return nil
	}
	values, _ := ctx.Value(valuesKey{}).(*Values)
	return values
}
