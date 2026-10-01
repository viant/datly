package dql

import (
	"encoding/json"
	"fmt"
	"strings"
)

// HandlerHeader is a SQL-free component declaration. Factory explicitly names a
// source-backed native constructor; legacy Type requires an application mapping.
type HandlerHeader struct {
	URI, Method, Name, Description       string
	Type, Factory, InputType, OutputType string
	Connector                            string
	MCPTool, Internal                    bool
	Declarative                          bool
}

// ParseHandlerSource recognizes a leading JSON header with Type or Factory.
// Unknown header fields are rejected rather than silently losing policy.
func parseLegacyHandlerSource(source string) (*HandlerHeader, string, error) {
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
		if !strings.Contains(header, `"Type"`) && !strings.Contains(header, `"Factory"`) {
			return nil, source, nil
		}
		return nil, "", fmt.Errorf("legacy component header: %w", err)
	}
	_, hasType := fields["Type"]
	_, hasFactory := fields["Factory"]
	if !hasType && !hasFactory {
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
	if result.URI == "" || result.Method == "" || result.Name == "" || (result.Type == "" && result.Factory == "") || result.InputType == "" || result.OutputType == "" {
		return nil, "", fmt.Errorf("handler requires URI, Method, Name, Type or Factory, InputType and OutputType")
	}
	return &result, text[end+2:], nil
}

// ParseHandlerSource accepts canonical DQL factory declarations and preserves
// legacy header parsing for existing sources. Canonical declarations retain
// their complete source so ordinary parameter and route diagnostics apply.
func ParseHandlerSource(source string) (*HandlerHeader, string, error) {
	legacy, body, err := parseLegacyHandlerSource(source)
	if err != nil {
		return nil, "", err
	}
	hasFactory := false
	for _, block := range extractDirectiveBlocks(body) {
		name, _, _, ok := parseDirectiveCall(block.body)
		if block.kind == directiveKindSetting && ok && strings.EqualFold(name, "handler_factory") {
			hasFactory = true
		}
	}
	prepared := PrepareSource(body)
	if hasFactory && prepared.Err() != nil {
		return nil, "", prepared.Err()
	}
	if legacy != nil {
		if hasFactory {
			return nil, "", fmt.Errorf("handler_factory cannot be combined with a legacy handler header")
		}
		return legacy, body, nil
	}
	d := prepared.Directives
	if !hasFactory {
		return nil, source, nil
	}
	if err := prepared.Err(); err != nil {
		return nil, "", err
	}
	if d.Route == nil || len(d.Route.Methods) != 1 {
		return nil, "", fmt.Errorf("handler_factory requires exactly one explicit route method")
	}
	if d.Settings.InputType == "" || d.Settings.OutputType == "" {
		return nil, "", fmt.Errorf("handler_factory requires explicit input_type and output_type")
	}
	return &HandlerHeader{URI: d.Route.URI, Method: d.Route.Methods[0], Name: d.HandlerName, Factory: d.HandlerFactory, InputType: d.Settings.InputType, OutputType: d.Settings.OutputType, Connector: d.Settings.DefaultConnector, Internal: d.Internal, Declarative: true}, source, nil
}
