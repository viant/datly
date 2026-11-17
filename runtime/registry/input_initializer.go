package registry

import (
	"fmt"
	"reflect"

	xhandler "github.com/viant/xdatly/handler"
)

// compileEmbeddedInitializer resolves one promoted initializer for an anonymous
// runtime input. Named inputs retain Go's existing method-set behavior.
func compileEmbeddedInitializer(typ reflect.Type) ([]int, error) {
	ordinary := reflect.TypeFor[xhandler.Initializer]()
	invoker := reflect.TypeFor[xhandler.InvokerInitializer]()
	implements := func(t reflect.Type) bool {
		return reflect.PointerTo(t).Implements(ordinary) || reflect.PointerTo(t).Implements(invoker)
	}
	if typ.Name() != "" || implements(typ) {
		return nil, nil
	}
	type candidate struct {
		typ  reflect.Type
		path []int
	}
	level := []candidate{{typ: typ}}
	for len(level) > 0 {
		var next []candidate
		var matches [][]int
		for _, parent := range level {
			for i := 0; i < parent.typ.NumField(); i++ {
				field := parent.typ.Field(i)
				if !field.Anonymous || field.PkgPath != "" || field.Type.Kind() != reflect.Struct {
					continue
				}
				path := append(append([]int(nil), parent.path...), i)
				if implements(field.Type) {
					matches = append(matches, path)
				} else {
					next = append(next, candidate{typ: field.Type, path: path})
				}
			}
		}
		if len(matches) > 1 {
			return nil, fmt.Errorf("anonymous input has ambiguous embedded initializers at depth %d", len(matches[0]))
		}
		if len(matches) == 1 {
			return matches[0], nil
		}
		level = next
	}
	return nil, nil
}

// InitializerTarget returns the preselected receiver on this invocation's input.
// It neither scans types nor allocates an embedded object during execution.
func (c *RouteInputContract) InitializerTarget(input any) any {
	if c == nil || c.contract == nil || len(c.contract.initializerPath) == 0 {
		return input
	}
	return reflect.ValueOf(input).Elem().FieldByIndex(c.contract.initializerPath).Addr().Interface()
}

func (c *RouteInputContract) HasInvokerInitializer() bool {
	return c != nil && c.contract != nil && c.contract.invokerInitializer
}
