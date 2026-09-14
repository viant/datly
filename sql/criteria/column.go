package criteria

import (
	"fmt"
	"reflect"
	"strconv"
	"time"
)

func (c Column) scalarType() reflect.Type {
	t := c.Type
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func (c Column) operator(op string) error {
	t := c.scalarType()
	if t == nil {
		return nil
	}
	switch op {
	case "=", "!=", "<>", "IN", "NOT IN", "IS", "IS NOT":
		return nil
	case "LIKE", "NOT LIKE":
		if t.Kind() == reflect.String {
			return nil
		}
	case ">", ">=", "<", "<=":
		if t == reflect.TypeOf(time.Time{}) || (t.Kind() >= reflect.Int && t.Kind() <= reflect.Float64) {
			return nil
		}
	}
	return fmt.Errorf("criteria operator %q is not supported for %v", op, t)
}

func (c Column) convert(value any) (any, error) {
	t := c.scalarType()
	if t == nil || value == nil {
		return value, nil
	}
	if t == reflect.TypeOf(time.Time{}) {
		if typed, ok := value.(time.Time); ok {
			return typed, nil
		}
		raw, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("criteria time requires a quoted value")
		}
		layout := c.TimeLayout
		if layout == "" {
			layout = time.RFC3339
		}
		return time.Parse(layout, raw)
	}
	result := reflect.New(t).Elem()
	raw := fmt.Sprint(value)
	switch t.Kind() {
	case reflect.String:
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("criteria string requires a quoted value")
		}
		result.SetString(text)
	case reflect.Bool:
		boolean, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("criteria boolean requires true or false")
		}
		result.SetBool(boolean)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if _, ok := value.(string); ok {
			return nil, fmt.Errorf("criteria integer requires a numeric value")
		}
		v, err := strconv.ParseInt(raw, 10, t.Bits())
		if err != nil {
			return nil, err
		}
		result.SetInt(v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if _, ok := value.(string); ok {
			return nil, fmt.Errorf("criteria integer requires a numeric value")
		}
		v, err := strconv.ParseUint(raw, 10, t.Bits())
		if err != nil {
			return nil, err
		}
		result.SetUint(v)
	case reflect.Float32, reflect.Float64:
		if _, ok := value.(string); ok {
			return nil, fmt.Errorf("criteria number requires a numeric value")
		}
		v, err := strconv.ParseFloat(raw, t.Bits())
		if err != nil {
			return nil, err
		}
		result.SetFloat(v)
	default:
		return nil, fmt.Errorf("unsupported criteria column type %v", t)
	}
	return result.Interface(), nil
}
