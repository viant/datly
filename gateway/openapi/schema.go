package openapi

import (
	"encoding"
	"encoding/json"
	"fmt"
	documentation "github.com/viant/datly/documentation"
	jsonmarshal "github.com/viant/structology/encoding/json/marshal"
	"reflect"
	"time"

	"github.com/viant/datly/gateway/openapi/openapi3"
)

type schemaKey struct {
	typeOf reflect.Type
	input  bool
	path   string
}

// schemaBuilder owns OpenAPI projection, not Go field/type discovery. All field
// enumeration comes from x/shape; unsupported JSON embedding fails explicitly.
type wireKey struct {
	shape *jsonmarshal.WireShape
	path  string
}
type schemaBuilder struct {
	wires       map[wireKey]*openapi3.Schema
	activeWires map[*jsonmarshal.WireShape]*openapi3.Schema
	identities  map[string]reflect.Type
	docs        *documentation.Snapshot
	scope, path string
	recursive   map[reflect.Type]string
	schemas     openapi3.Schemas
	names       map[schemaKey]string
	owners      map[string]schemaKey
	active      map[schemaKey]bool
}

func newSchemaBuilder(schemas openapi3.Schemas) *schemaBuilder {
	return &schemaBuilder{wires: map[wireKey]*openapi3.Schema{}, activeWires: map[*jsonmarshal.WireShape]*openapi3.Schema{}, identities: map[string]reflect.Type{}, recursive: map[reflect.Type]string{}, schemas: schemas, names: map[schemaKey]string{}, owners: map[string]schemaKey{}, active: map[schemaKey]bool{}}
}

func (b *schemaBuilder) schema(t reflect.Type, input bool) (*openapi3.Schema, error) {
	if t == nil {
		return nil, fmt.Errorf("schema type is missing")
	}
	if t.Kind() == reflect.Pointer {
		result, err := b.schema(t.Elem(), input)
		if err != nil {
			return nil, err
		}
		return b.nullable(result), nil
	}
	if t == reflect.TypeFor[time.Time]() {
		return &openapi3.Schema{Type: "string", Format: "date-time"}, nil
	}
	if t.Implements(reflect.TypeFor[json.Marshaler]()) || reflect.PointerTo(t).Implements(reflect.TypeFor[json.Marshaler]()) ||
		t.Implements(reflect.TypeFor[encoding.TextMarshaler]()) || reflect.PointerTo(t).Implements(reflect.TypeFor[encoding.TextMarshaler]()) ||
		(input && (reflect.PointerTo(t).Implements(reflect.TypeFor[json.Unmarshaler]()) || reflect.PointerTo(t).Implements(reflect.TypeFor[encoding.TextUnmarshaler]()))) {
		return nil, fmt.Errorf("type %s uses custom wire serialization without schema authority", t)
	}
	key := schemaKey{t, input, b.path}
	if name, ok := b.names[key]; ok {
		return &openapi3.Schema{Ref: "#/components/schemas/" + name}, nil
	}
	if b.active[key] {
		return nil, fmt.Errorf("recursive non-object type %s cannot be represented", t)
	}
	b.active[key] = true
	defer delete(b.active, key)
	s := &openapi3.Schema{}
	switch t.Kind() {
	case reflect.Bool:
		s.Type = "boolean"
	case reflect.String:
		s.Type = "string"
	case reflect.Int8, reflect.Int16, reflect.Int32:
		s.Type = "integer"
		s.Format = "int32"
	case reflect.Int, reflect.Int64:
		s.Type = "integer"
		s.Format = "int64"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s.Type = "integer"
		zero := float64(0)
		s.Min = &zero
	case reflect.Float32:
		s.Type = "number"
		s.Format = "float"
	case reflect.Float64:
		s.Type = "number"
		s.Format = "double"
	case reflect.Slice, reflect.Array:
		if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.Uint8 {
			s.Type = "string"
			s.Format = "byte"
			s.Nullable = true
			break
		}
		s.Type = "array"
		var err error
		s.Items, err = b.schema(t.Elem(), input)
		if err != nil {
			return nil, err
		}
		if t.Kind() == reflect.Slice {
			s.Nullable = true
		} else {
			size := uint64(t.Len())
			s.MinItems = size
			s.MaxItems = &size
		}
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("map %s has non-string keys", t)
		}
		s.Type = "object"
		s.Nullable = true
		var err error
		s.AdditionalProperties, err = b.schema(t.Elem(), input)
		if err != nil {
			return nil, err
		}
	case reflect.Interface:
		if t.NumMethod() != 0 {
			return nil, fmt.Errorf("interface %s has no concrete wire contract", t)
		}
		// An empty schema describes arbitrary JSON, including null.
	case reflect.Struct:
		return b.object(key)
	default:
		return nil, fmt.Errorf("type %s cannot be represented as JSON", t)
	}
	return s, nil
}

func (b *schemaBuilder) nullable(s *openapi3.Schema) *openapi3.Schema {
	if s.Ref != "" {
		// A 3.0 Reference Object cannot have nullable siblings. A null-only branch
		// preserves both reference reuse and nullability under 3.0 schema semantics.
		return &openapi3.Schema{AnyOf: openapi3.SchemaList{s, {Type: "object", Nullable: true, Enum: []interface{}{nil}}}}
	}
	result := *s
	if result.Type != "" {
		result.Nullable = true
	}
	return &result
}
