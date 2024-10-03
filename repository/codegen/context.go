package codegen

import "context"

var generatorKey string

func IsGeneratorContext(ctx context.Context) bool {
	return ctx.Value(generatorKey) != nil
}

func WithGeneratorContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, generatorKey, true)
}
