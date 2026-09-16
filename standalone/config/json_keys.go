package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

// validateJSONKeys runs before decoding into maps, where repeated keys would
// overwrite earlier definitions. Errors deliberately omit keys and values.
func validateJSONKeys(data []byte, target reflect.Type) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	fields := map[reflect.Type]jsonFields{}
	var value func(reflect.Type) error
	value = func(typ reflect.Type) error {
		typ = jsonObjectType(typ)
		token, err := decoder.Token()
		if err != nil {
			return fmt.Errorf("invalid JSON configuration")
		}
		delimiter, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delimiter {
		case '{':
			if typ != nil && typ.Kind() == reflect.Struct {
				if _, ok := fields[typ]; !ok {
					fields[typ] = jsonStructFields(typ)
				}
			}
			seen := map[string]bool{}
			for decoder.More() {
				token, err := decoder.Token()
				key, ok := token.(string)
				if err != nil || !ok {
					return fmt.Errorf("invalid JSON configuration")
				}
				var child reflect.Type
				if typ != nil && typ.Kind() == reflect.Map {
					child = typ.Elem()
				} else if field := fields[typ].lookup(key); field != nil {
					key, child = field.name, field.typ
				}
				if seen[key] {
					return fmt.Errorf("invalid configuration: duplicate JSON key")
				}
				seen[key] = true
				if err := value(child); err != nil {
					return err
				}
			}
		case '[':
			var child reflect.Type
			if typ != nil && (typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array) {
				child = typ.Elem()
			}
			for decoder.More() {
				if err := value(child); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("invalid JSON configuration")
		}
		if _, err := decoder.Token(); err != nil {
			return fmt.Errorf("invalid JSON configuration")
		}
		return nil
	}
	if err := value(target); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("configuration must contain one document")
	}
	return nil
}
