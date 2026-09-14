// Package velty adapts cached typed Velty programs to the shared handler
// engine. Binding, DI scope, transaction completion, and output finalization
// remain engine-owned.
package velty

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	rhandler "github.com/viant/datly/runtime/handler"
)

type Config struct {
	Template string
}

type Handler[I any, O any] struct {
	program      *compiledProgram[Context, I, O]
	captureInput func(context.Context, *I) (any, error)
}

func New[I any, O any](config Config) (*Handler[I, O], error) {
	if strings.TrimSpace(config.Template) == "" {
		return nil, fmt.Errorf("velty handler template is required")
	}
	if inputType := reflect.TypeFor[I](); inputType == nil || inputType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("velty handler input must be a struct, got %v", inputType)
	}
	if outputType := reflect.TypeFor[O](); outputType == nil || outputType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("velty handler output must be a struct, got %v", outputType)
	}
	program, err := cachedCompiledProgram[Context, I, O](config.Template)
	if err != nil {
		return nil, fmt.Errorf("compile velty handler: %w", err)
	}
	return &Handler[I, O]{program: program}, nil
}

func (h *Handler[I, O]) InputType() reflect.Type {
	return reflect.TypeFor[I]()
}

func (h *Handler[I, O]) OutputType() reflect.Type {
	return reflect.TypeFor[O]()
}

func (h *Handler[I, O]) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	if h == nil || h.program == nil {
		return nil, fmt.Errorf("velty handler is not initialized")
	}
	input, ok := invocation.Input.(*I)
	if !ok || input == nil {
		return nil, fmt.Errorf("velty handler input must be *%s, got %T", reflect.TypeFor[I](), invocation.Input)
	}
	programContext, err := newContext(ctx, invocation.Binder, h.program.capabilities)
	if err != nil {
		return nil, err
	}
	if programContext.WriteHooks != nil {
		programContext.WriteHooks.input = input
	}
	output := new(O)
	if err := h.program.Exec(programContext, input, output); err != nil {
		return output, err
	}
	return output, nil
}
