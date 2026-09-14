package dql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/viant/sqlparser/query"
)

func splitHintAndSQL(raw string) (map[string]any, string, error) {
	text := strings.TrimSpace(raw)
	if text == "" {
		return nil, "", nil
	}
	text = stripDeclarationControlPrefix(text)
	text = strings.TrimSpace(text)
	if !strings.HasPrefix(text, "{") {
		return nil, text, nil
	}
	var hint map[string]any
	decoder := json.NewDecoder(strings.NewReader(text))
	if err := decoder.Decode(&hint); err != nil {
		return nil, "", fmt.Errorf("parse declaration hint %q: %w", text, err)
	}
	offset := decoder.InputOffset()
	return hint, strings.TrimSpace(text[offset:]), nil
}

func stripDeclarationControlPrefix(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return text
	}
	switch text[0] {
	case '?':
		return strings.TrimSpace(text[1:])
	case '!':
		text = text[1:]
		if strings.HasPrefix(text, "!") {
			text = text[1:]
		}
		i := 0
		for i < len(text) && text[i] >= '0' && text[i] <= '9' {
			i++
		}
		return strings.TrimSpace(text[i:])
	default:
		return text
	}
}

func decodeDataTypeHint(hint map[string]any) string {
	if len(hint) == 0 {
		return ""
	}
	if value, ok := hint["DataType"].(string); ok {
		return value
	}
	return ""
}

func decodeProjectionDataType(list query.List) string {
	if len(list) == 0 || list[0] == nil {
		return ""
	}
	comment := strings.TrimSpace(list[0].Comments)
	if comment == "" {
		return ""
	}
	comment = strings.TrimPrefix(comment, "/*")
	comment = strings.TrimSuffix(comment, "*/")
	comment = strings.TrimSpace(comment)
	if comment == "" {
		return ""
	}
	var hint map[string]any
	if err := json.NewDecoder(bytes.NewBufferString(comment)).Decode(&hint); err != nil {
		return ""
	}
	return decodeDataTypeHint(hint)
}

func normalizeDataType(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "bool", "boolean":
		return "bool"
	case "string":
		return "string"
	case "int", "integer":
		return "int"
	case "int64":
		return "int64"
	case "float64", "double", "float":
		return "float64"
	case "float32":
		return "float32"
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
