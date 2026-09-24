// Package custom adapts typed user-defined Go handlers to the shared runtime
// handler contract. It owns no binding, routing, SQL, or transaction behavior.
package custom

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/bindly"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/tagly/tags"
	xhandler "github.com/viant/xdatly/handler"
)

type contractHandler[I any, O any] struct {
	contract xhandler.Contract[I, O]
	bindOnce sync.Once
	bindErr  error
}

// BindStatic binds explicitly tagged fields of a pointer contract once,
// before the registered handler serves any invocation. Value contracts and
// contracts without bind tags need no initialization.
func (h *contractHandler[I, O]) BindStatic(ctx context.Context, injector *bindly.Injector) error {
	if h == nil || h.contract == nil {
		return fmt.Errorf("custom handler contract is required")
	}
	typeOf := reflect.TypeOf(h.contract)
	isPointer := typeOf.Kind() == reflect.Pointer
	structType := typeOf
	if isPointer {
		structType = typeOf.Elem()
	}
	if structType.Kind() != reflect.Struct {
		return nil
	}
	hasBindings := false
	for _, field := range reflect.VisibleFields(structType) {
		if raw, ok := field.Tag.Lookup("bind"); ok && field.IsExported() {
			var kind string
			if err := tags.Values(raw).MatchPairs(func(key, value string) error {
				if key == "kind" {
					kind = value
				}
				return nil
			}); err != nil {
				return fmt.Errorf("handler field %s binding: %w", field.Name, err)
			}
			if kind == "" || invocationBindingKind(kind) {
				return fmt.Errorf("handler field %s binding kind %q is request-scoped; bind it on input", field.Name, kind)
			}
			hasBindings = true
		}
	}
	if !hasBindings {
		return nil
	}
	if !isPointer {
		return fmt.Errorf("static handler bindings require a pointer contract: %s", typeOf)
	}
	h.bindOnce.Do(func() {
		if injector == nil {
			h.bindErr = fmt.Errorf("custom handler injector is required")
			return
		}
		h.bindErr = injector.Bind(ctx, h.contract)
	})
	return h.bindErr
}

func invocationBindingKind(kind string) bool {
	switch kind {
	case "header", "query", "path", "cookie", "form", "body", "input", "param", "component", "caller_output":
		return true
	default:
		return false
	}
}

// New adapts the public binder-aware custom Go contract to the unified runtime
// handler. The engine has already bound and initialized input before Execute.
func New[I any, O any](contract xhandler.Contract[I, O]) rhandler.TypedHandler {
	return &contractHandler[I, O]{contract: contract}
}

func (h *contractHandler[I, O]) InputType() reflect.Type {
	return reflect.TypeFor[I]()
}

func (h *contractHandler[I, O]) OutputType() reflect.Type {
	return reflect.TypeFor[O]()
}

func (h *contractHandler[I, O]) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	if h == nil || h.contract == nil {
		return nil, fmt.Errorf("custom handler contract is required")
	}
	input, ok := invocation.Input.(*I)
	if !ok || input == nil {
		return nil, fmt.Errorf("custom handler input must be *%s, got %T", reflect.TypeFor[I](), invocation.Input)
	}
	output := new(O)
	session := newSession(invocation.Binder, invocation.Response)
	ctx = xhandler.WithSession(ctx, session)
	return output, h.contract.Exec(ctx, session, input, output)
}
