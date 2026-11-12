package session

import (
	"context"
	"fmt"
)

type outputProjectionKey struct{}

type OutputProjection struct {
	View       string
	Output     interface{}
	Fields     []string
	FieldsHint bool
}

func ContextWithOutputProjection(ctx context.Context, output interface{}) context.Context {
	return ContextWithViewOutputProjection(ctx, "", output)
}

func ContextWithViewOutputProjection(ctx context.Context, viewName string, output interface{}) context.Context {
	return context.WithValue(ctx, outputProjectionKey{}, &OutputProjection{View: viewName, Output: output})
}

func ContextWithOutputFields(ctx context.Context, fields ...string) context.Context {
	return ContextWithViewOutputFields(ctx, "", fields...)
}

func ContextWithViewOutputFields(ctx context.Context, viewName string, fields ...string) context.Context {
	return context.WithValue(ctx, outputProjectionKey{}, &OutputProjection{View: viewName, Fields: append([]string(nil), fields...), FieldsHint: true})
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

func OutputFieldsFromContext(ctx context.Context, viewName string) []string {
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
		return append([]string(nil), actual.Fields...)
	case OutputProjection:
		if actual.View != "" && normalizeViewProjectionName(actual.View) != normalizeViewProjectionName(viewName) {
			return nil
		}
		return append([]string(nil), actual.Fields...)
	}
	return nil
}

func OutputFieldsHintFromContext(ctx context.Context, viewName string) bool {
	if ctx == nil {
		return false
	}
	value := ctx.Value(outputProjectionKey{})
	switch actual := value.(type) {
	case *OutputProjection:
		if actual == nil {
			return false
		}
		if actual.View != "" && normalizeViewProjectionName(actual.View) != normalizeViewProjectionName(viewName) {
			return false
		}
		return actual.FieldsHint
	case OutputProjection:
		if actual.View != "" && normalizeViewProjectionName(actual.View) != normalizeViewProjectionName(viewName) {
			return false
		}
		return actual.FieldsHint
	}
	return false
}

func EmptyOutputFieldsError(viewName string) error {
	return fmt.Errorf("output projection for view %s did not specify any field names", viewName)
}
