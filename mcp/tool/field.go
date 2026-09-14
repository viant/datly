package tool

import (
	"reflect"
	"strings"
)

func publicFieldName(logicalName, sourceName string, field reflect.StructField) (string, bool) {
	if field.Tag.Get("setMarker") == "true" {
		return "", true
	}
	if value, ok := field.Tag.Lookup("json"); ok {
		name, _, _ := strings.Cut(value, ",")
		if name == "-" {
			return "", true
		}
		if name != "" {
			return name, false
		}
	}
	if logicalName = strings.TrimSpace(logicalName); logicalName != "" {
		return logicalName, false
	}
	if sourceName = strings.TrimSpace(sourceName); sourceName != "" {
		return sourceName, false
	}
	return field.Name, false
}

func jsonField(field reflect.StructField) (name string, hidden bool) {
	if field.Tag.Get("setMarker") == "true" {
		return "", true
	}
	name = field.Name
	if value, ok := field.Tag.Lookup("json"); ok {
		parts := strings.Split(value, ",")
		if parts[0] == "-" {
			return "", true
		}
		if parts[0] != "" {
			name = parts[0]
		}
	}
	return name, false
}
