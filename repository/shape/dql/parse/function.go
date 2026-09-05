package parse

import (
	"fmt"
	"strings"
)

// FunctionHandler handles parsed DQL function call.
type FunctionHandler interface {
	Name() string
	Handle(call *FunctionCall, result *Result) error
}

// FunctionHandlerFunc adapts function to handler.
type FunctionHandlerFunc struct {
	FunctionName string
	Fn           func(call *FunctionCall, result *Result) error
}

func (f FunctionHandlerFunc) Name() string {
	return strings.ToLower(strings.TrimSpace(f.FunctionName))
}

func (f FunctionHandlerFunc) Handle(call *FunctionCall, result *Result) error {
	if f.Fn == nil {
		return nil
	}
	return f.Fn(call, result)
}

// FunctionRegistry stores handlers by function name.
type FunctionRegistry struct {
	items map[string]FunctionHandler
}

// NewFunctionRegistry creates function registry.
func NewFunctionRegistry(handlers ...FunctionHandler) *FunctionRegistry {
	ret := &FunctionRegistry{items: map[string]FunctionHandler{}}
	for _, handler := range handlers {
		ret.Register(handler)
	}
	return ret
}

// Register registers function handler.
func (r *FunctionRegistry) Register(handler FunctionHandler) {
	if r == nil || handler == nil {
		return
	}
	name := strings.ToLower(strings.TrimSpace(handler.Name()))
	if name == "" {
		return
	}
	r.items[name] = handler
}

func (r *FunctionRegistry) apply(call *FunctionCall, result *Result) error {
	if r == nil || call == nil {
		return nil
	}
	handler := r.items[strings.ToLower(call.Name)]
	if handler == nil {
		return nil
	}
	if err := handler.Handle(call, result); err != nil {
		return fmt.Errorf("function %s failed: %w", call.Name, err)
	}
	call.Handled = true
	return nil
}
