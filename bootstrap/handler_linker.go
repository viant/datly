package bootstrap

import (
	"fmt"
	"reflect"
	"strings"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
)

type handlerLinker struct {
	artifact *Artifact
	registry *x.Registry
}

func (l handlerLinker) reference() (string, error) {
	component := l.artifact.Component
	context := &typecatalog.ResolutionContext{PackagePath: component.Key.Scope}
	if authored := component.TypeContext; authored != nil {
		context.DefaultPackage = authored.DefaultPackage
		for _, item := range authored.Imports {
			context.Imports = append(context.Imports, typecatalog.PackageImport{Alias: item.Alias, Package: item.Package})
		}
	}
	context = typecatalog.NormalizeContext(context)
	resolver := xshape.Resolver{Package: component.Key.Scope, Imports: map[string]string{}}
	if context != nil {
		if resolver.Package == "" {
			resolver.Package = context.DefaultPackage
		}
		for _, item := range context.Imports {
			resolver.Imports[item.Alias] = item.Package
		}
	}
	key := ""
	for _, route := range component.Routes {
		// Blank means unspecified: the component's single named handler is
		// inherited by its other routes, as with explicit RegisteredComponent.Handler.
		if route == nil || strings.TrimSpace(route.Handler) == "" {
			continue
		}
		reference, err := resolver.Reference(route.Handler)
		if err != nil {
			return "", fmt.Errorf("handler factory reference: %w", err)
		}
		if len(reference.Wrappers) > 0 || len(reference.Arguments) > 0 || !reference.Exported() {
			return "", fmt.Errorf("handler factory reference %q must name an exported function", route.Handler)
		}
		candidate, err := resolver.Canonical(route.Handler)
		if err != nil {
			return "", err
		}
		if key != "" && key != candidate {
			return "", fmt.Errorf("component %s has conflicting named handler factories", component.Key.String())
		}
		key = candidate
	}
	return key, nil
}

func (l handlerLinker) resolve(key string) (rhandler.TypedHandler, error) {
	function, ok := l.registry.LookupFunction(key)
	if !ok {
		return nil, fmt.Errorf("linked handler factory %q is not registered", key)
	}
	if err := l.signature(function); err != nil {
		return nil, err
	}
	values, err := function.Call()
	if err != nil {
		return nil, fmt.Errorf("link handler: %w", err)
	}
	if len(values) == 2 && values[1] != nil {
		return nil, fmt.Errorf("linked handler factory %s: %w", key, values[1].(error))
	}
	handler, ok := values[0].(rhandler.TypedHandler)
	if !ok || (xshape.Runtime{}).IsNil(handler) {
		return nil, fmt.Errorf("linked handler factory %q returned nil", key)
	}
	if err := l.contract(key, handler); err != nil {
		return nil, err
	}
	return handler, nil
}

func (handlerLinker) signature(function *x.Function) error {
	typ := function.Type()
	handlerType := reflect.TypeOf((*rhandler.TypedHandler)(nil)).Elem()
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if typ.NumIn() != 0 || typ.IsVariadic() || typ.NumOut() < 1 || typ.NumOut() > 2 || !typ.Out(0).Implements(handlerType) || typ.NumOut() == 2 && typ.Out(1) != errorType {
		return fmt.Errorf("linked handler factory %q must return TypedHandler or (TypedHandler,error); register its typed linking bridge", function.Key())
	}
	return nil
}

func (l handlerLinker) contract(key string, handler rhandler.TypedHandler) error {
	if handler.InputType() != l.artifact.inputType || handler.OutputType() != l.artifact.outputType {
		return fmt.Errorf("linked handler factory %q contract mismatch: got (%v,%v), want (%v,%v)", key, handler.InputType(), handler.OutputType(), l.artifact.inputType, l.artifact.outputType)
	}
	return nil
}
