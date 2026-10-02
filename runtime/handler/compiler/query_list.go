package compiler

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/xform/conv"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	xexec "github.com/viant/xdatly/exec"
	"reflect"
	"strconv"
	"strings"
)

// queryListTransformer expands primitive query lists by default. HTTP wire
// occurrences remain ordered; already typed protocol arrays keep their type.
type queryListTransformer struct {
	target     reflect.Type
	allowEmpty bool
	strict     bool // HTTP paths retain their existing conversion policy.
}

func (t *queryListTransformer) WireSourceType() reflect.Type { return t.target }
func (t *queryListTransformer) Transform(ctx context.Context, _ locator.Resolver, raw any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if typed, ok := raw.(handlerprovider.TypedQueryValue); ok {
		if typed.Value == nil || !reflect.TypeOf(typed.Value).AssignableTo(t.target) {
			return nil, fmt.Errorf("typed query value requires %s, got %T", t.target, typed.Value)
		}
		return typed.Value, nil
	}
	if raw != nil && reflect.TypeOf(raw).AssignableTo(t.target) && (t.target.Elem().Kind() != reflect.String || nativeToolArguments(ctx)) {
		return raw, nil
	}
	var occurrences []string
	switch value := raw.(type) {
	case string:
		occurrences = []string{value}
	case []string:
		occurrences = value
	default:
		if raw != nil && primitiveListItem(reflect.TypeOf(raw)) {
			return (conv.ValueConverter{}).Convert([]any{raw}, t.target)
		}
		return (conv.ValueConverter{}).Convert(raw, t.target)
	}
	if t.allowEmpty && len(occurrences) == 1 && occurrences[0] == "" {
		return reflect.MakeSlice(t.target, 0, 0).Interface(), nil
	}
	tokens := []string{}
	for _, occurrence := range occurrences {
		if t.strict {
			tokens = append(tokens, strings.Split(occurrence, ",")...)
			continue
		}
		// Original Datly normalizes a single CSV value, but converts bare
		// repeated occurrences directly. CSV occurrences in repeated keys are
		// the supported merging extension and use the same CSV normalization.
		if len(occurrences) == 1 || strings.Contains(occurrence, ",") || queryListEnclosed(occurrence) {
			tokens = append(tokens, legacyQueryListTokens(occurrence, t.target.Elem().Kind() != reflect.String)...)
		} else {
			tokens = append(tokens, occurrence)
		}
	}
	result := reflect.MakeSlice(t.target, len(tokens), len(tokens))
	for index, token := range tokens {
		if t.strict && token == "" {
			return nil, &bindinput.Error{Cause: fmt.Errorf("query list item %d is empty", index)}
		}
		item, err := (conv.ValueConverter{}).Convert(token, t.target.Elem())
		if !t.strict && legacyQueryInteger(t.target.Elem().Kind()) {
			item, err = legacyQueryListInteger(token, t.target.Elem())
		}
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

// queryListEnclosed deliberately matches the original first/last-character
// check; whitespace outside the enclosure is not removed first.
func queryListEnclosed(raw string) bool {
	return len(raw) >= 2 && raw[0] == '[' && raw[len(raw)-1] == ']'
}

func legacyQueryListTokens(raw string, trim bool) []string {
	if raw == "" {
		return nil
	}
	if queryListEnclosed(raw) {
		raw = raw[1 : len(raw)-1]
	}
	items := strings.Split(raw, ",")
	if !trim {
		return items
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		if item = strings.TrimSpace(item); item != "" {
			result = append(result, item)
		}
	}
	return result
}

func legacyQueryInteger(kind reflect.Kind) bool {
	return kind == reflect.Int || kind == reflect.Int64 || kind == reflect.Uint || kind == reflect.Uint64
}

// Original integer lists parse scientific/fractional notation through a machine
// int before widening or unsigned conversion. Keep that behavior for query wire
// text; smaller integer kinds retain the existing range-checked converter.
func legacyQueryListInteger(raw string, target reflect.Type) (any, error) {
	var number int
	if floating, err := strconv.ParseFloat(raw, 64); err == nil {
		number = int(floating)
	} else {
		var err error
		if number, err = strconv.Atoi(raw); err != nil {
			return nil, err
		}
	}
	result := reflect.New(target).Elem()
	if target.Kind() == reflect.Uint || target.Kind() == reflect.Uint64 {
		result.SetUint(uint64(uint(number)))
	} else {
		result.SetInt(int64(number))
	}
	return result.Interface(), nil
}

func applyQueryList(field reflect.StructField, paramCSV bool, binding *bindly.BindingSpec) error {
	tag, hasTag := field.Tag.Lookup("queryList")
	if hasTag && tag != "csv" {
		return fmt.Errorf("queryList must be csv")
	}
	explicit := paramCSV || hasTag
	if !explicit && binding.Location.Kind == "path" {
		if field.Type.Kind() == reflect.Slice && primitiveListItem(field.Type.Elem()) && field.Type.Elem().Kind() != reflect.Uint8 && binding.Transformer == nil {
			binding.SourceType = reflect.TypeFor[any]()
			binding.Transformer = &pathListTransformer{target: field.Type}
		}
		return nil
	}
	if !explicit && (binding.Location.Kind != "query" || field.Type.Kind() != reflect.Slice || !primitiveListItem(field.Type.Elem()) || binding.Transformer != nil) {
		return nil
	}
	if binding.Location.Kind != "query" {
		return fmt.Errorf("queryList csv requires a query binding")
	}
	if field.Type.Kind() != reflect.Slice {
		return fmt.Errorf("queryList csv requires a typed slice")
	}
	if !primitiveListItem(field.Type.Elem()) {
		return fmt.Errorf("queryList csv requires scalar list items")
	}
	if binding.Transformer != nil {
		return fmt.Errorf("queryList csv cannot replace an explicit codec")
	}
	binding.SourceType = reflect.TypeFor[any]()
	binding.Transformer = &queryListTransformer{target: field.Type, allowEmpty: binding.Required != nil && !*binding.Required}
	return nil
}

func primitiveListItem(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.String, reflect.Bool:
		return true
	}
	return false
}

// Tool arguments have already been validated as native typed arrays before
// projection into request providers. URI and HTTP query values are wire text.
func nativeToolArguments(ctx context.Context) bool {
	invocation := xexec.GetContext(ctx)
	return invocation != nil && invocation.Method == "tools/call"
}

// pathListTransformer preserves HTTP CSV paths and native tool JSON arrays.
// An explicit codec remains authoritative and does not use this default.
type pathListTransformer struct{ target reflect.Type }

func (t *pathListTransformer) WireSourceType() reflect.Type { return t.target }
func (t *pathListTransformer) Transform(ctx context.Context, resolver locator.Resolver, raw any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !nativeToolArguments(ctx) {
		return (&queryListTransformer{target: t.target, strict: true}).Transform(ctx, resolver, raw)
	}
	if raw != nil && reflect.TypeOf(raw).AssignableTo(t.target) {
		return raw, nil
	}
	wire, ok := raw.(string)
	if !ok {
		return nil, &bindinput.Error{Cause: fmt.Errorf("native path collection requires JSON array, got %T", raw)}
	}
	if strings.TrimSpace(wire) == "null" {
		return nil, &bindinput.Error{Cause: fmt.Errorf("native path collection requires a non-null array")}
	}
	value := reflect.New(t.target)
	if err := json.Unmarshal([]byte(wire), value.Interface()); err != nil {
		return nil, &bindinput.Error{Cause: fmt.Errorf("native path collection: %w", err)}
	}
	return value.Elem().Interface(), nil
}
