package session

import "context"

type outputProjectionKey struct{}

type OutputProjection struct {
	View   string
	Output interface{}
}

func ContextWithOutputProjection(ctx context.Context, output interface{}) context.Context {
	return ContextWithViewOutputProjection(ctx, "", output)
}

func ContextWithViewOutputProjection(ctx context.Context, viewName string, output interface{}) context.Context {
	return context.WithValue(ctx, outputProjectionKey{}, &OutputProjection{View: viewName, Output: output})
}

func OutputProjectionFromContext(ctx context.Context, viewName string) interface{} {
	if ctx == nil {
		return nil
	}
	value := ctx.Value(outputProjectionKey{})
	switch actual := value.(type) {
	case *OutputProjection:
		if actual == nil {
			return nil
		}
		if actual.View != "" && normalizeViewProjectionName(actual.View) != normalizeViewProjectionName(viewName) {
			return nil
		}
		return actual.Output
	case OutputProjection:
		if actual.View != "" && normalizeViewProjectionName(actual.View) != normalizeViewProjectionName(viewName) {
			return nil
		}
		return actual.Output
	}
	return nil
}
