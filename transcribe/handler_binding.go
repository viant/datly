package transcribe

import (
	"fmt"
	"go/token"
	"reflect"
	"runtime"
	"strings"

	xhandler "github.com/viant/xdatly/handler"
)

// HandlerMapping explicitly connects legacy names to an existing v1 factory.
// DestinationPackage owns registration artifacts, never the handwritten logic.
type HandlerMapping struct {
	LegacyType, LegacyInput, LegacyOutput string
	FactoryPackage, FactoryName           string
	DestinationPackage                    string
	// UnusedParameters explicitly acknowledges obsolete legacy declarations.
	// Each entry requires a nonempty explanation and may not hide a bound field.
	UnusedParameters map[string]string
}

// HandlerBinding is immutable compiled type authority. Construct it with
// NewHandlerBinding; constructors are inspected, never executed.
type HandlerBinding struct {
	mapping       HandlerMapping
	input, output reflect.Type
}

func NewHandlerBinding[I, O any](mapping HandlerMapping, factory any) (*HandlerBinding, error) {
	expected := reflect.TypeFor[func() xhandler.Contract[I, O]]()
	if reflect.TypeOf(factory) != expected || reflect.ValueOf(factory).IsNil() {
		return nil, fmt.Errorf("handler factory must have exact signature %v", expected)
	}
	input, output := reflect.TypeFor[I](), reflect.TypeFor[O]()
	for _, typ := range []reflect.Type{input, output} {
		if typ.Kind() != reflect.Struct || !token.IsExported(typ.Name()) || typ.PkgPath() == "" {
			return nil, fmt.Errorf("handler contract must be an exported named struct: %v", typ)
		}
	}
	if mapping.LegacyType == "" || mapping.LegacyInput == "" || mapping.LegacyOutput == "" || mapping.FactoryPackage == "" || mapping.DestinationPackage == "" || !token.IsIdentifier(mapping.FactoryName) || !token.IsExported(mapping.FactoryName) {
		return nil, fmt.Errorf("handler mapping requires legacy names, destination package, and an exported factory identity")
	}
	if mapping.DestinationPackage == mapping.FactoryPackage || mapping.DestinationPackage == input.PkgPath() || mapping.DestinationPackage == output.PkgPath() {
		return nil, fmt.Errorf("handler registration destination must be separate from handwritten factory and contract packages")
	}
	identity := mapping.FactoryPackage + "." + mapping.FactoryName
	function := runtime.FuncForPC(reflect.ValueOf(factory).Pointer())
	if function == nil || function.Name() != identity {
		return nil, fmt.Errorf("handler factory does not resolve to %s", identity)
	}
	unused := make(map[string]string, len(mapping.UnusedParameters))
	for name, reason := range mapping.UnusedParameters {
		if name == "" || strings.TrimSpace(reason) == "" {
			return nil, fmt.Errorf("unused handler parameter requires a name and explanation")
		}
		unused[name] = reason
	}
	mapping.UnusedParameters = unused
	return &HandlerBinding{mapping: mapping, input: input, output: output}, nil
}
