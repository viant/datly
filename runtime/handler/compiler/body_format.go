package compiler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
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
	plan, err := compileBodyFormat(field.Type, map[reflect.Type]*bodyFormatPlan{})
	if err != nil {
		return fmt.Errorf("body field %s format: %w", field.Name, err)
	}
	if plan == nil || !plan.hasLayout(map[*bodyFormatPlan]bool{}) {
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
	name string
	plan *bodyFormatPlan
}
type bodyJSONField struct {
	name   string
	tagged bool
	index  []int
	field  reflect.StructField
}

func compileBodyFormat(target reflect.Type, memo map[reflect.Type]*bodyFormatPlan) (*bodyFormatPlan, error) {
	for target.Kind() == reflect.Pointer {
		target = target.Elem()
	}
	if target == reflect.TypeFor[time.Time]() {
		return nil, nil
	}
	// A reusable type's explicit JSON method owns its representation. Authored
	// time.Time leaves are selected by their enclosing binding field instead.
	unmarshaler := reflect.TypeFor[json.Unmarshaler]()
	if target.Implements(unmarshaler) || reflect.PointerTo(target).Implements(unmarshaler) {
		return nil, nil
	}

	if target.Kind() == reflect.Slice || target.Kind() == reflect.Array || target.Kind() == reflect.Map {
		if plan, ok := memo[target]; ok {
			return plan, nil
		}
		plan := &bodyFormatPlan{}
		memo[target] = plan
		item, err := compileBodyFormat(target.Elem(), memo)
		if err != nil {
			return nil, err
		}
		plan.item = item
		return plan, nil
	}
	if target.Kind() != reflect.Struct {
		return nil, nil
	}
	if plan, ok := memo[target]; ok {
		return plan, nil
	}
	plan := &bodyFormatPlan{}
	memo[target] = plan
	for _, candidate := range authorizedBodyJSONFields(target) {
		field := candidate.field
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
			child, err = compileBodyFormat(field.Type, memo)
			if err != nil {
				return nil, err
			}
		}
		// Unformatted fields remain in the name graph so JSON's exact-name
		// preference and embedding dominance cannot authorize another field.
		plan.fields = append(plan.fields, bodyFormatField{name: candidate.name, plan: child})
	}
	return plan, nil
}
func (p *bodyFormatPlan) hasLayout(seen map[*bodyFormatPlan]bool) bool {
	if p == nil || seen[p] {
		return false
	}
	seen[p] = true
	if p.layout != "" {
		return true
	}
	if p.item != nil && p.item.hasLayout(seen) {
		return true
	}
	for _, field := range p.fields {
		if field.plan.hasLayout(seen) {
			return true
		}
	}
	return false
}
func authorizedBodyJSONFields(target reflect.Type) []bodyJSONField {
	var candidates []bodyJSONField
	var collect func(reflect.Type, []int, map[reflect.Type]bool)
	collect = func(typ reflect.Type, index []int, visiting map[reflect.Type]bool) {
		if visiting[typ] {
			return
		}
		visiting[typ] = true
		defer delete(visiting, typ)
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			child := field.Type
			for child.Kind() == reflect.Pointer {
				child = child.Elem()
			}
			if field.Tag.Get("setMarker") == "true" || field.PkgPath != "" && (!field.Anonymous || child.Kind() != reflect.Struct) {
				continue
			}
			name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
			if name == "-" {
				continue
			}
			position := append(append([]int{}, index...), i)
			if field.Anonymous && name == "" && child.Kind() == reflect.Struct && child != reflect.TypeFor[time.Time]() {
				collect(child, position, visiting)
				continue
			}
			if field.PkgPath != "" {
				continue
			}
			tagged := name != ""
			if name == "" {
				name = field.Name
			}
			candidates = append(candidates, bodyJSONField{name: name, tagged: tagged, index: position, field: field})
		}
	}
	collect(target, nil, map[reflect.Type]bool{})
	groups := map[string][]bodyJSONField{}
	for _, field := range candidates {
		groups[field.name] = append(groups[field.name], field)
	}
	var result []bodyJSONField
	for _, fields := range groups {
		depth := len(fields[0].index)
		for _, field := range fields {
			if len(field.index) < depth {
				depth = len(field.index)
			}
		}
		var shallow, tagged []bodyJSONField
		for _, field := range fields {
			if len(field.index) == depth {
				shallow = append(shallow, field)
				if field.tagged {
					tagged = append(tagged, field)
				}
			}
		}
		if len(tagged) == 1 {
			result = append(result, tagged[0])
		} else if len(tagged) == 0 && len(shallow) == 1 {
			result = append(result, shallow[0])
		}
	}
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i].index, result[j].index
		for n := 0; n < len(a) && n < len(b); n++ {
			if a[n] != b[n] {
				return a[n] < b[n]
			}
		}
		return len(a) < len(b)
	})
	return result
}
func (p *bodyFormatPlan) field(name string) *bodyFormatPlan {
	for _, field := range p.fields {
		if field.name == name {
			return field.plan
		}
	}
	for _, field := range p.fields {
		if strings.EqualFold(field.name, name) {
			return field.plan
		}
	}
	return nil
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
	if p == nil || !p.hasLayout(map[*bodyFormatPlan]bool{}) {
		return raw, nil
	}
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

	trimmed := strings.TrimSpace(string(raw))
	if p.item != nil && strings.HasPrefix(trimmed, "[") {
		return rewriteBodyJSONValues(raw, func(_ string, index int, value json.RawMessage) (json.RawMessage, error) {
			return p.item.normalize(value, fmt.Sprintf("%s[%d]", path, index))
		})
	}
	return rewriteBodyJSONValues(raw, func(key string, _ int, value json.RawMessage) (json.RawMessage, error) {
		child := p.item
		if child == nil {
			child = p.field(key)
		}
		if child == nil {
			return value, nil
		}
		return child.normalize(value, path+"."+key)
	})
}

// Preserve lexical key order and duplicates; ordinary typed JSON decoding owns
// last-wins and case-variant selection. Only authorized value spans are replaced.
func rewriteBodyJSONValues(raw json.RawMessage, convert func(string, int, json.RawMessage) (json.RawMessage, error)) (json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok || (delimiter != '{' && delimiter != '[') {
		return nil, fmt.Errorf("expected JSON object or array")
	}
	result := make([]byte, 0, len(raw))
	previous := int64(0)
	index := 0
	for decoder.More() {
		key := ""
		if delimiter == '{' {
			token, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			var ok bool
			key, ok = token.(string)
			if !ok {
				return nil, fmt.Errorf("expected JSON key")
			}
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		end := decoder.InputOffset()
		start := end - int64(len(value))
		result = append(result, raw[previous:start]...)
		normalized, err := convert(key, index, value)
		if err != nil {
			return nil, err
		}
		result = append(result, normalized...)
		previous = end
		index++
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	result = append(result, raw[previous:]...)
	return result, nil
}
