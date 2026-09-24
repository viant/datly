package dql

import (
	"encoding/json"
	"fmt"
	"strings"
)

// HandlerHeader is the SQL-free legacy component declaration. Handler adaptation
// is supplied separately by the application; Type is not a constructor name.
type HandlerHeader struct {
	URI, Method, Name, Description string
	Type, InputType, OutputType    string
	Connector                      string
	MCPTool, Internal              bool
}

// ParseHandlerSource recognizes a leading legacy JSON header with a Type.
// Unknown header fields are rejected rather than silently losing policy.
func ParseHandlerSource(source string) (*HandlerHeader, string, error) {
	text := strings.TrimSpace(source)
	if !strings.HasPrefix(text, "/*") {
		return nil, source, nil
	}
	end := strings.Index(text, "*/")
	if end < 0 {
		return nil, source, nil
	}
	header := strings.TrimSpace(text[2:end])
	if !strings.HasPrefix(header, "{") {
		return nil, source, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(header), &fields); err != nil {
		if !strings.Contains(header, `"Type"`) {
			return nil, source, nil
		}
		return nil, "", fmt.Errorf("legacy component header: %w", err)
	}
	if _, ok := fields["Type"]; !ok {
		return nil, source, nil
	}
	// encoding/json otherwise accepts repeated (including differently cased)
	// declarations, silently letting the last value change route policy.
	scanner := json.NewDecoder(strings.NewReader(header))
	_, _ = scanner.Token() // json.Unmarshal above already validated syntax.
	seen := map[string]bool{}
	for scanner.More() {
		key, _ := scanner.Token()
		name := strings.ToLower(key.(string))
		if seen[name] {
			return nil, "", fmt.Errorf("duplicate legacy handler header field %q", key)
		}
		seen[name] = true
		var value json.RawMessage
		_ = scanner.Decode(&value)
	}
	var result HandlerHeader
	decoder := json.NewDecoder(strings.NewReader(header))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return nil, "", fmt.Errorf("legacy handler header: %w", err)
	}
	if result.URI == "" || result.Method == "" || result.Name == "" || result.Type == "" || result.InputType == "" || result.OutputType == "" {
		return nil, "", fmt.Errorf("legacy handler requires URI, Method, Name, Type, InputType and OutputType")
	}
	return &result, text[end+2:], nil
}
