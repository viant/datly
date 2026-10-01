package compiler

import (
	"context"
	"fmt"
	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/xform/conv"
	"reflect"
	"strings"
)

// queryListTransformer expands only explicitly opted-in query lists. HTTP wire
// occurrences remain ordered; already typed protocol arrays keep their type.
type queryListTransformer struct{ target reflect.Type }

func (t *queryListTransformer) WireSourceType() reflect.Type { return t.target }
func (t *queryListTransformer) Transform(ctx context.Context, _ locator.Resolver, raw any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if raw != nil && reflect.TypeOf(raw).AssignableTo(t.target) {
		return raw, nil
	}
	var occurrences []string
	switch value := raw.(type) {
	case string:
		occurrences = []string{value}
	case []string:
		occurrences = value
	default:
		return (conv.ValueConverter{}).Convert(raw, t.target)
	}
	tokens := []string{}
	for _, occurrence := range occurrences {
		tokens = append(tokens, strings.Split(occurrence, ",")...)
	}
	result := reflect.MakeSlice(t.target, len(tokens), len(tokens))
	for index, token := range tokens {
		if token == "" {
			return nil, &bindinput.Error{Cause: fmt.Errorf("query list item %d is empty", index)}
		}
		item, err := (conv.ValueConverter{}).Convert(token, t.target.Elem())
		if err != nil {
			return nil, &bindinput.Error{Cause: fmt.Errorf("query list item %d: %w", index, err)}
		}
		value := reflect.ValueOf(item)
		if !value.IsValid() {
			return nil, &bindinput.Error{Cause: fmt.Errorf("query list item %d is null", index)}
		}
		result.Index(index).Set(value)
	}
	return result.Interface(), nil
}
func applyQueryList(field reflect.StructField, paramCSV bool, binding *bindly.BindingSpec) error {
	tag, hasTag := field.Tag.Lookup("queryList")
	if hasTag && tag != "csv" {
		return fmt.Errorf("queryList must be csv")
	}
	if !paramCSV && !hasTag {
		return nil
	}
	if binding.Location.Kind != "query" {
		return fmt.Errorf("queryList csv requires a query binding")
	}
	if field.Type.Kind() != reflect.Slice {
		return fmt.Errorf("queryList csv requires a typed slice")
	}
	typ := field.Type.Elem()
	switch typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.String, reflect.Bool:
	default:
		return fmt.Errorf("queryList csv requires scalar list items")
	}
	if binding.Transformer != nil {
		return fmt.Errorf("queryList csv cannot replace an explicit codec")
	}
	binding.SourceType = reflect.TypeFor[any]()
	binding.Transformer = &queryListTransformer{target: field.Type}
	return nil
}
