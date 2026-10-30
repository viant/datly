package fragment

import (
	"fmt"
	"reflect"
)

// NullableIn is an opt-in selection predicate. An absent or empty selection
// omits the predicate; a NULL member selects SQL NULL, alongside any bound
// non-NULL values. The expression is trusted authored SQL, never client input.
func (c *Context) NullableIn(expression string, selection any) (string, error) {
	value := reflect.ValueOf(selection)
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return "", nil
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return "", nil
	}
	if value.Kind() != reflect.Slice && value.Kind() != reflect.Array {
		return "", fmt.Errorf("nullable IN requires a selection array")
	}
	if value.Len() == 0 {
		return "", nil
	}
	values := make([]any, 0, value.Len())
	hasNull := false
	for i := 0; i < value.Len(); i++ {
		item := value.Index(i)
		for item.IsValid() && (item.Kind() == reflect.Pointer || item.Kind() == reflect.Interface) {
			if item.IsNil() {
				item = reflect.Value{}
				break
			}
			item = item.Elem()
		}
		if !item.IsValid() {
			hasNull = true
		} else {
			values = append(values, item.Interface())
		}
	}
	if len(values) == 0 {
		return "(" + expression + ") IS NULL", nil
	}
	clause, err := c.in("("+expression+")", values, true)
	if err != nil || !hasNull {
		return clause, err
	}
	return "(" + clause + " OR (" + expression + ") IS NULL)", nil
}
