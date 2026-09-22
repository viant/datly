package tool

import (
	"fmt"
	"reflect"
	"strings"
)

type mcpField struct {
	name    string
	aliases []string
	hidden  bool
}

func publicFieldName(logicalName, sourceName string, field reflect.StructField) (string, []string, bool, error) {
	mcp, err := mcpFieldMetadata(field)
	if err != nil {
		return "", nil, false, err
	}
	if field.Tag.Get("setMarker") == "true" {
		return "", nil, true, nil
	}
	if mcp.hidden {
		return "", nil, true, nil
	}
	if mcp.name != "" {
		return mcp.name, mcp.aliases, false, nil
	}
	if value, ok := field.Tag.Lookup("json"); ok {
		name, _, _ := strings.Cut(value, ",")
		if name == "-" {
			return "", nil, true, nil
		}
		if name != "" {
			return name, mcp.aliases, false, nil
		}
	}
	if logicalName = strings.TrimSpace(logicalName); logicalName != "" {
		return logicalName, mcp.aliases, false, nil
	}
	if sourceName = strings.TrimSpace(sourceName); sourceName != "" {
		return sourceName, mcp.aliases, false, nil
	}
	return field.Name, mcp.aliases, false, nil
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

func mcpFieldMetadata(field reflect.StructField) (mcpField, error) {
	value := strings.TrimSpace(field.Tag.Get("mcp"))
	if value == "" {
		return mcpField{}, nil
	}
	if value == "-" {
		return mcpField{hidden: true}, nil
	}
	result := mcpField{}
	for _, item := range strings.Split(value, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		key, raw, ok := strings.Cut(item, "=")
		if !ok {
			return mcpField{}, fmt.Errorf("unsupported mcp tag option %q on field %s", item, field.Name)
		}
		key = strings.TrimSpace(key)
		raw = strings.TrimSpace(raw)
		switch key {
		case "name":
			result.name = raw
		case "aliases":
			result.aliases = splitMCPAliases(raw)
		default:
			return mcpField{}, fmt.Errorf("unsupported mcp tag option %q on field %s", key, field.Name)
		}
	}
	return result, nil
}

func splitMCPAliases(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == '|' || r == ';'
	})
	result := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || seen[part] {
			continue
		}
		seen[part] = true
		result = append(result, part)
	}
	return result
}
