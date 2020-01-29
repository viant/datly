package generic

import (
	"github.com/viant/toolbox"
	"strings"
)

//Index represents Index function
type Index func(values interface{}) string

//NewIndex returns an Index for supplied keys
func NewIndex(keys []string) Index {
	return func(values interface{}) string {
		object, isObject := values.(*Object)
		var result []string
		if isObject {
			result = indexWithObject(keys, object)
		} else {
			result = indexFromMap(keys, toolbox.AsMap(values))
		}
		return strings.Join(result, "/")
	}
}

func indexWithObject(keys []string, object *Object) []string {
	var result = make([]string, 0)
	for _, key := range keys {
		value := object.Value(key)
		if value == nil {
			result = append(result, "")
			continue
		}
		if text, ok := value.(string); ok {
			result = append(result, text)
			continue
		}
		result = append(result, toolbox.AsString(value))
	}
	return result
}

func indexFromMap(keys []string, values map[string]interface{}) []string {
	var result = make([]string, 0)
	for _, key := range keys {
		value, ok := values[key]
		if !ok {
			result = append(result, "")
			continue
		}
		if text, ok := value.(string); ok {
			result = append(result, text)
			continue
		}
		result = append(result, toolbox.AsString(value))
	}
	return result
}
