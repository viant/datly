package compiler

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/bindly/locator"
	bodyprovider "github.com/viant/bindly/provider/body"
	"github.com/viant/tagly/format"
)

// applyBodyFormat keeps nested authored date layouts in the canonical binding
// plan. The body is decoded to its original Go type and set markers by Bindly;
// no parallel input shape or application preprocessing owns this conversion.
func applyBodyFormat(field reflect.StructField, binding *bindly.BindingSpec) error {
	if binding.Transformer != nil || binding.Location.Kind != "body" {
		return nil
	}
	plan, err := compileBodyFormat(field.Type, map[reflect.Type]bool{})
	if err != nil {
		return fmt.Errorf("body field %s format: %w", field.Name, err)
	}
	if plan == nil {
		return nil
	}
	binding.SourceType = reflect.TypeFor[json.RawMessage]()
	binding.Transformer = &bodyFormatTransformer{target: field.Type, plan: plan}
	return nil
}

type bodyFormatPlan struct {
	layout string
	item   *bodyFormatPlan
	fields []bodyFormatField
}
type bodyFormatField struct {
	names []string
	plan  *bodyFormatPlan
}

func compileBodyFormat(target reflect.Type, visiting map[reflect.Type]bool) (*bodyFormatPlan, error) {
	for target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	if target == reflect.TypeFor[time.Time]() {
		return nil, nil
	}
	if target.Kind() == reflect.Slice || target.Kind() == reflect.Array || target.Kind() == reflect.Map {
		item, err := compileBodyFormat(target.Elem(), visiting)
		if err != nil || item == nil {
			return nil, err
		}
		return &bodyFormatPlan{item: item}, nil
	}
	if target.Kind() != reflect.Struct || visiting[target] {
		return nil, nil
	}
	visiting[target] = true
	defer delete(visiting, target)
	plan := &bodyFormatPlan{}
	for i := 0; i < target.NumField(); i++ {
		field := target.Field(i)
		if field.PkgPath != "" || field.Tag.Get("setMarker") == "true" {
			continue
		}
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		names := []string{name}
		if name != field.Name {
			names = append(names, field.Name)
		}
		typ := field.Type
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		var child *bodyFormatPlan
		if typ == reflect.TypeFor[time.Time]() {
			formatting, err := format.Parse(field.Tag)
			if err != nil {
				return nil, err
			}
			if formatting != nil && formatting.TimeLayout != "" {
				child = &bodyFormatPlan{layout: formatting.TimeLayout}
			}
		} else {
			var err error
			child, err = compileBodyFormat(field.Type, visiting)
			if err != nil {
				return nil, err
			}
		}
		if child != nil {
			plan.fields = append(plan.fields, bodyFormatField{names: names, plan: child})
		}
	}
	if len(plan.fields) == 0 {
		return nil, nil
	}
	return plan, nil
}

type bodyFormatTransformer struct {
	target reflect.Type
	plan   *bodyFormatPlan
}

// WireSourceType retains the original JSON body contract for documentation and
// MCP projection while the binding requests raw bytes for authored conversion.
func (t *bodyFormatTransformer) WireSourceType() reflect.Type { return t.target }
func (t *bodyFormatTransformer) Transform(ctx context.Context, _ locator.Resolver, input any) (any, error) {
	if input == nil {
		return nil, nil
	}
	if reflect.TypeOf(input).AssignableTo(t.target) {
		return input, nil
	}
	var raw []byte
	switch value := input.(type) {
	case json.RawMessage:
		raw = value
	case []byte:
		raw = value
	default:
		return nil, &bindinput.Error{Cause: fmt.Errorf("JSON body requires raw encoded data, got %T", input)}
	}
	normalized, err := t.plan.normalize(raw, "")
	if err != nil {
		return nil, &bindinput.Error{Cause: err}
	}
	// Reuse the same typed decoder and presence projection as ordinary bodies.
	source, err := bodyprovider.New(normalized, "application/json", nil)
	if err != nil {
		return nil, err
	}
	value, _, err := source.Value(ctx, t.target, "")
	return value, err
}
func (p *bodyFormatPlan) normalize(raw json.RawMessage, path string) (json.RawMessage, error) {
	if strings.TrimSpace(string(raw)) == "null" {
		return raw, nil
	}
	if p.layout != "" {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, fmt.Errorf("%s: expected date string: %w", path, err)
		}
		stamp, err := time.Parse(p.layout, text)
		if err != nil {
			stamp, err = time.Parse(time.RFC3339Nano, text)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		return json.Marshal(stamp.Format(time.RFC3339Nano))
	}
	if p.item != nil {
		trimmed := strings.TrimSpace(string(raw))
		if strings.HasPrefix(trimmed, "{") {
			var values map[string]json.RawMessage
			if err := json.Unmarshal(raw, &values); err != nil {
				return nil, err
			}
			for key, value := range values {
				normalized, err := p.item.normalize(value, path+"["+key+"]")
				if err != nil {
					return nil, err
				}
				values[key] = normalized
			}
			return json.Marshal(values)
		}
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return nil, err
		}
		for i, value := range values {
			normalized, err := p.item.normalize(value, fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return nil, err
			}
			values[i] = normalized
		}
		return json.Marshal(values)
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, err
	}
	for _, field := range p.fields {
		for key, value := range values {
			matched := false
			for _, name := range field.names {
				if strings.EqualFold(key, name) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
			normalized, err := field.plan.normalize(value, path+"."+key)
			if err != nil {
				return nil, err
			}
			values[key] = normalized
		}
	}
	return json.Marshal(values)
}
