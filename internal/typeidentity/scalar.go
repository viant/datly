package typeidentity

import (
	"encoding"
	"encoding/json"
	"go/types"
	"reflect"
)

// ScalarPair is a discovery-only nullability change over one exact Go leaf.
type ScalarPair struct {
	Type     string
	Nullable bool
}

func scalarPair(equal bool, wirePointer, destinationPointer bool, representation string, serializer bool) (ScalarPair, bool) {
	if !equal || wirePointer == destinationPointer || representation == "" || serializer {
		return ScalarPair{}, false
	}
	return ScalarPair{Type: representation, Nullable: wirePointer}, true
}

func representation(kind reflect.Kind) string {
	switch kind {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return "integer"
	}
	return ""
}

// ReflectScalarPair requires resolved identities from the caller's type owner.
func ReflectScalarPair(wire, destination reflect.Type) (ScalarPair, bool) {
	if wire == nil || destination == nil {
		return ScalarPair{}, false
	}
	wp, dp := wire.Kind() == reflect.Pointer, destination.Kind() == reflect.Pointer
	if wp {
		wire = wire.Elem()
	}
	if dp {
		destination = destination.Elem()
	}
	if wire.Kind() == reflect.Pointer || destination.Kind() == reflect.Pointer {
		return ScalarPair{}, false
	}
	serializer := false
	for _, t := range []reflect.Type{wire, reflect.PointerTo(wire)} {
		for _, contract := range []reflect.Type{reflect.TypeFor[json.Marshaler](), reflect.TypeFor[json.Unmarshaler](), reflect.TypeFor[encoding.TextMarshaler](), reflect.TypeFor[encoding.TextUnmarshaler]()} {
			serializer = serializer || t.Implements(contract)
		}
	}
	return scalarPair(wire == destination, wp, dp, representation(wire.Kind()), serializer)
}

// SourceScalarPair uses go/types identities already resolved in source scope.
func SourceScalarPair(wire, destination types.Type) (ScalarPair, bool) {
	if wire == nil || destination == nil {
		return ScalarPair{}, false
	}
	wire, destination = types.Unalias(wire), types.Unalias(destination)
	wp, wok := wire.(*types.Pointer)
	dp, dok := destination.(*types.Pointer)
	if wok {
		wire = types.Unalias(wp.Elem())
	}
	if dok {
		destination = types.Unalias(dp.Elem())
	}
	if _, ok := wire.(*types.Pointer); ok {
		return ScalarPair{}, false
	}
	if _, ok := destination.(*types.Pointer); ok {
		return ScalarPair{}, false
	}
	basic, ok := wire.Underlying().(*types.Basic)
	if !ok {
		return ScalarPair{}, false
	}
	schema := ""
	switch {
	case basic.Info()&types.IsString != 0:
		schema = "string"
	case basic.Info()&types.IsBoolean != 0:
		schema = "boolean"
	case basic.Info()&types.IsInteger != 0:
		schema = "integer"
	}
	serializer := false
	bytes := types.NewSlice(types.Typ[types.Byte])
	failure := types.Universe.Lookup("error").Type()
	variable := func(t types.Type) *types.Var { return types.NewVar(0, nil, "", t) }
	marshal := types.NewSignatureType(nil, nil, nil, types.NewTuple(), types.NewTuple(variable(bytes), variable(failure)), false)
	unmarshal := types.NewSignatureType(nil, nil, nil, types.NewTuple(variable(bytes)), types.NewTuple(variable(failure)), false)
	for _, t := range []types.Type{wire, types.NewPointer(wire)} {
		for name, expected := range map[string]*types.Signature{"MarshalJSON": marshal, "MarshalText": marshal, "UnmarshalJSON": unmarshal, "UnmarshalText": unmarshal} {
			method, _, _ := types.LookupFieldOrMethod(t, false, nil, name)
			if method != nil && types.Identical(method.Type(), expected) {
				serializer = true
			}
		}
	}
	return scalarPair(types.Identical(wire, destination), wok, dok, schema, serializer)
}
