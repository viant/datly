package compiler

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/xform/conv"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/format"
)

// Keep authored date parsing in the input plan, so HTTP and internal providers
// use the same conversion. Explicit codecs retain their own source contract.
func applyTimeFormat(field reflect.StructField, binding *bindly.BindingSpec) error {
	if binding.Transformer != nil {
		return nil
	}
	typ := field.Type
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ != reflect.TypeFor[time.Time]() {
		return nil
	}
	formatting, err := format.Parse(field.Tag)
	if err != nil {
		return fmt.Errorf("input field %s format: %w", field.Name, err)
	}
	if formatting == nil || formatting.TimeLayout == "" {
		return nil
	}
	// Preserve both wire strings and already-typed child input until conversion.
	binding.SourceType = reflect.TypeFor[any]()
	transformer := &timeFormatTransformer{target: field.Type, layout: formatting.TimeLayout}
	// Match Bindly's ordinary conversion policy; internal sources are not
	// automatically client errors merely because their destination is a date.
	switch binding.Location.Kind {
	case "query", "path", "header", "cookie", "form", "body":
		transformer.external = true
	}
	binding.Transformer = transformer
	return nil
}

type timeFormatTransformer struct {
	target   reflect.Type
	layout   string
	external bool
}

func (t *timeFormatTransformer) WireSchema() *spec.WireSchema {
	if t == nil {
		return nil
	}
	return &spec.WireSchema{Type: "string", Format: "date"}
}

func (t *timeFormatTransformer) Transform(_ context.Context, _ locator.Resolver, input any) (any, error) {
	value, err := (conv.ValueConverter{TimeLayout: t.layout}).Convert(input, t.target)
	if err != nil && t.external {
		return nil, &bindinput.Error{Cause: err}
	}
	return value, err
}
