package openapi

import (
	"fmt"
	"reflect"

	"github.com/viant/datly/gateway/openapi/openapi3"
	xshape "github.com/viant/x/shape"
)

// input selects request-provider encoding rather than applying JSON byte rules
// to repeated query/form values. Conversion itself remains owned by Bindly.
func (b *schemaBuilder) input(t reflect.Type, kind string) (*openapi3.Schema, error) {
	schema, err := b.schema(t, true)
	if err != nil {
		return nil, err
	}
	if kind != "query" && kind != "form" {
		return schema, nil
	}
	if t.Kind() != reflect.Slice && t.Kind() != reflect.Array {
		return schema, nil
	}
	// Bindly treats the exact []byte type specially (one base64 value); named
	// byte slices are ordinary repeated numeric collections. Keep the former
	// explicit unsupported boundary rather than pretending both have one encoding.
	if t == reflect.TypeFor[[]byte]() {
		return nil, fmt.Errorf("%s byte input requires explicit wire encoding support", kind)
	}
	element, err := b.schema((xshape.Runtime{}).Indirect(t.Elem()), true)
	if err != nil {
		return nil, err
	}
	switch element.Type {
	case "string", "integer", "number", "boolean":
	default:
		return nil, fmt.Errorf("%s array requires scalar items", kind)
	}
	projected := *schema
	projected.Type = "array"
	projected.Format = ""
	projected.Items = element
	projected.Nullable = false
	// Preserve the original fixed-array bounds and future schema constraints.
	return &projected, nil
}
