package spec

import "strings"

// EffectiveParameters returns authored parameters after #define declarations
// replace declaration-shaped #set entries for the same logical data point.
func EffectiveParameters(parameters []*Parameter) []*Parameter {
	defined := map[string]bool{}
	for _, parameter := range parameters {
		if parameter != nil && parameter.Declaration == DeclarationKindDefine {
			defined[parameter.Identity()] = true
		}
	}
	if len(defined) == 0 {
		return parameters
	}
	result := make([]*Parameter, 0, len(parameters))
	for _, parameter := range parameters {
		if parameter != nil && parameter.Declaration == DeclarationKindSet && defined[parameter.Identity()] {
			continue
		}
		result = append(result, parameter)
	}
	return result
}

// IsTransportInput reports whether a parameter is populated by request-scoped
// transport providers rather than a named runtime capability.
func (p *Parameter) IsTransportInput() bool {
	if p == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(p.Source.Kind)) {
	case "http_request", "query", "path", "header", "cookie", "form", "body":
		return true
	default:
		return false
	}
}

// Identity returns the canonical key for one authored data point. Input and
// output declarations may intentionally share a name, so name alone is not an
// identity.
func (p *Parameter) Identity() string {
	if p == nil {
		return ""
	}
	kind := strings.ToLower(strings.TrimSpace(p.Source.Kind))
	name := strings.ToLower(strings.TrimSpace(p.Source.Name))
	if p.IsDerivedOutput() {
		name = "derived"
	}
	if p.EmitOutput && kind != "logger" && kind != "const" {
		kind = "output"
		if name == "" {
			name = "body"
		}
	}
	key := strings.ToLower(strings.TrimSpace(p.Name)) + "|" + kind + "|" + name
	if p.EmitOutput && (kind == "logger" || kind == "const") {
		key += "|output"
	}
	return key
}
