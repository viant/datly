// Package transform compiles reusable paths and transfers values through
// Bindly's typed destination binding. It has no handler or transport ownership.
package transform

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// Path is an immutable path over Go structs or JSON-like maps and slices.
type Path struct {
	raw     string
	tokens  []string
	indexes [][]int
}

// CompilePointer compiles an RFC 6901 pointer. The empty pointer names the root.
func CompilePointer(raw string) (*Path, error) {
	if raw == "" {
		return &Path{raw: raw}, nil
	}
	if !strings.HasPrefix(raw, "/") {
		return nil, fmt.Errorf("JSON pointer %q must start with '/'", raw)
	}
	parts := strings.Split(raw[1:], "/")
	for i, part := range parts {
		for n := 0; n < len(part); n++ {
			if part[n] == '~' {
				if n+1 >= len(part) || (part[n+1] != '0' && part[n+1] != '1') {
					return nil, fmt.Errorf("JSON pointer %q has an invalid escape", raw)
				}
				n++
			}
		}
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
	}
	return &Path{raw: raw, tokens: parts}, nil
}

// CompileSelector compiles an exact dot/bracket path such as Scope.Items[0].ID.
// Empty segments, forgiving separators and implicit roots are rejected.
func CompileSelector(raw string) (*Path, error) {
	if raw == "" {
		return nil, fmt.Errorf("selector is required")
	}
	var parts []string
	for i := 0; i < len(raw); {
		start := i
		for i < len(raw) && raw[i] != '.' && raw[i] != '[' && raw[i] != ']' {
			i++
		}
		if i > start {
			part := raw[start:i]
			if strings.TrimSpace(part) != part || part == "" {
				return nil, fmt.Errorf("invalid selector %q", raw)
			}
			parts = append(parts, part)
		} else if i == 0 || raw[i] != '[' {
			return nil, fmt.Errorf("invalid selector %q", raw)
		}
		for i < len(raw) && raw[i] == '[' {
			i++
			start = i
			for i < len(raw) && raw[i] != ']' {
				i++
			}
			if i == len(raw) || !canonicalIndex(raw[start:i]) {
				return nil, fmt.Errorf("invalid selector index in %q", raw)
			}
			parts = append(parts, raw[start:i])
			i++
		}
		if i == len(raw) {
			break
		}
		if raw[i] != '.' || i+1 == len(raw) || raw[i+1] == '.' || raw[i+1] == ']' || raw[i+1] == '[' {
			return nil, fmt.Errorf("invalid selector %q", raw)
		}
		i++
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("invalid selector %q", raw)
	}
	return &Path{raw: raw, tokens: parts}, nil
}

func canonicalIndex(token string) bool {
	if token == "" || (len(token) > 1 && token[0] == '0') {
		return false
	}
	for i := range token {
		if token[i] < '0' || token[i] > '9' {
			return false
		}
	}
	_, err := strconv.Atoi(token)
	return err == nil
}

func (p *Path) String() string {
	if p == nil {
		return ""
	}
	return p.raw
}

// ValidateType checks every statically known segment. Dynamic interface and
// string-map contents are checked by Select at invocation time.
func (p *Path) ValidateType(root reflect.Type) error {
	_, err := p.ForType(root)
	return err
}

// ForType returns a separate immutable path with struct field indexes planned
// for one declared source type. The caller's Path is never modified.
func (p *Path) ForType(root reflect.Type) (*Path, error) {
	if p == nil || root == nil {
		return nil, fmt.Errorf("path and source type are required")
	}
	bound := &Path{raw: p.raw, tokens: append([]string(nil), p.tokens...), indexes: make([][]int, len(p.tokens))}
	current := root
	for i, token := range p.tokens {
		for current.Kind() == reflect.Pointer {
			current = current.Elem()
		}
		switch current.Kind() {
		case reflect.Interface:
			return bound, nil
		case reflect.Map:
			if current.Key().Kind() != reflect.String {
				return nil, fmt.Errorf("path %q requires string map keys", p.raw)
			}
			current = current.Elem()
		case reflect.Slice, reflect.Array:
			if !canonicalIndex(token) {
				return nil, fmt.Errorf("path %q has invalid array index %q", p.raw, token)
			}
			current = current.Elem()
		case reflect.Struct:
			field, ok := current.FieldByName(token)
			if !ok || !field.IsExported() {
				return nil, fmt.Errorf("input path %q does not exist in %s", p.raw, root)
			}
			bound.indexes[i] = append([]int(nil), field.Index...)
			current = field.Type
		default:
			return nil, fmt.Errorf("path %q descends into %s", p.raw, current)
		}
	}
	return bound, nil
}

// Select distinguishes absent values from present nulls. It never coerces a
// scalar, guesses a root, or treats a signed index as an object key on arrays.
func (p *Path) Select(root any) (any, bool, error) {
	if p == nil {
		return nil, false, fmt.Errorf("path is required")
	}
	value := reflect.ValueOf(root)
	for tokenIndex, token := range p.tokens {
		for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
			if value.IsNil() {
				return nil, false, nil
			}
			value = value.Elem()
		}
		if !value.IsValid() {
			return nil, false, nil
		}
		switch value.Kind() {
		case reflect.Map:
			if value.Type().Key().Kind() != reflect.String {
				return nil, false, fmt.Errorf("path %q requires string map keys", p.raw)
			}
			value = value.MapIndex(reflect.ValueOf(token).Convert(value.Type().Key()))
			if !value.IsValid() {
				return nil, false, nil
			}
		case reflect.Slice, reflect.Array:
			if !canonicalIndex(token) {
				return nil, false, fmt.Errorf("path %q has invalid array index %q", p.raw, token)
			}
			index, _ := strconv.Atoi(token)
			if index >= value.Len() {
				return nil, false, nil
			}
			value = value.Index(index)
		case reflect.Struct:
			indexes := []int(nil)
			if tokenIndex < len(p.indexes) {
				indexes = p.indexes[tokenIndex]
			}
			if indexes == nil {
				field, ok := value.Type().FieldByName(token)
				if !ok || !field.IsExported() {
					return nil, false, nil
				}
				indexes = field.Index
			}
			var found bool
			value, found = safeField(value, indexes)
			if !found || !value.CanInterface() {
				return nil, false, nil
			}
		default:
			return nil, false, fmt.Errorf("path %q descends into %s", p.raw, value.Kind())
		}
	}
	if !value.IsValid() {
		return nil, false, nil
	}
	for value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil, true, nil
		}
		value = value.Elem()
	}
	if (value.Kind() == reflect.Pointer || value.Kind() == reflect.Map || value.Kind() == reflect.Slice) && value.IsNil() {
		return nil, true, nil
	}
	return value.Interface(), true, nil
}

func safeField(value reflect.Value, indexes []int) (reflect.Value, bool) {
	for _, index := range indexes {
		for value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return reflect.Value{}, false
			}
			value = value.Elem()
		}
		if value.Kind() != reflect.Struct || index >= value.NumField() {
			return reflect.Value{}, false
		}
		value = value.Field(index)
	}
	return value, true
}

// Assign writes into a JSON-like object. It creates object parents only;
// arrays must already exist and conflicting/scalar paths fail explicitly.
func (p *Path) Assign(root map[string]any, value any) error {
	if p == nil || root == nil || len(p.tokens) == 0 {
		return fmt.Errorf("non-root path and object are required")
	}
	var current any = root
	for i, token := range p.tokens {
		last := i == len(p.tokens)-1
		switch container := current.(type) {
		case map[string]any:
			if last {
				if _, exists := container[token]; exists {
					return fmt.Errorf("path %q is assigned more than once", p.raw)
				}
				container[token] = value
				return nil
			}
			next, exists := container[token]
			if !exists {
				next = map[string]any{}
				container[token] = next
			}
			current = next
		case []any:
			if !canonicalIndex(token) {
				return fmt.Errorf("path %q has invalid array index %q", p.raw, token)
			}
			index, _ := strconv.Atoi(token)
			if index >= len(container) {
				return fmt.Errorf("path %q array index %d is out of bounds", p.raw, index)
			}
			if last {
				container[index] = value
				return nil
			}
			current = container[index]
		default:
			return fmt.Errorf("path %q descends into %T", p.raw, current)
		}
	}
	return nil
}
