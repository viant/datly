package route

import (
	"fmt"
	"net/url"
	"strings"
)

type pathSegment struct {
	value       string
	placeholder bool
}

// PathTemplate is an immutable compiled route path.
type PathTemplate struct {
	path       string
	segments   []pathSegment
	parameters []string
}

// CompilePathTemplate compiles an authored absolute route path.
func CompilePathTemplate(path string) (*PathTemplate, error) {
	path = strings.TrimSpace(path)
	if path == "" || path[0] != '/' {
		return nil, fmt.Errorf("route path must be absolute")
	}
	if strings.ContainsAny(path, "?#") {
		return nil, fmt.Errorf("route path must not contain query or fragment")
	}
	parts := splitPath(path)
	segments := make([]pathSegment, len(parts))
	parameters := make([]string, 0, len(parts))
	seen := map[string]bool{}
	for i, part := range parts {
		if strings.HasPrefix(part, "{") || strings.HasSuffix(part, "}") {
			if len(part) < 3 || part[0] != '{' || part[len(part)-1] != '}' {
				return nil, fmt.Errorf("malformed route placeholder %q", part)
			}
			name := part[1 : len(part)-1]
			if !isPlaceholderName(name) {
				return nil, fmt.Errorf("invalid route placeholder %q", name)
			}
			if seen[name] {
				return nil, fmt.Errorf("duplicate route placeholder %q", name)
			}
			seen[name] = true
			segments[i] = pathSegment{value: name, placeholder: true}
			parameters = append(parameters, name)
			continue
		}
		if strings.ContainsAny(part, "{}") {
			return nil, fmt.Errorf("malformed route segment %q", part)
		}
		segments[i] = pathSegment{value: part}
	}
	return &PathTemplate{path: path, segments: segments, parameters: parameters}, nil
}

// Path returns the authored path.
func (t *PathTemplate) Path() string {
	if t == nil {
		return ""
	}
	return t.path
}

// Parameters returns placeholder names in authored order.
func (t *PathTemplate) Parameters() []string {
	if t == nil {
		return nil
	}
	return append([]string(nil), t.parameters...)
}

// EscapedTemplatePath returns an RFC6570-compatible escaped path while
// preserving authored placeholders.
func (t *PathTemplate) EscapedTemplatePath() string {
	if t == nil {
		return ""
	}
	parts := make([]string, len(t.segments))
	for i, segment := range t.segments {
		if segment.placeholder {
			parts[i] = "{" + segment.value + "}"
			continue
		}
		parts[i] = url.PathEscape(segment.value)
	}
	return "/" + strings.Join(parts, "/")
}

// MatchEscapedPath matches an escaped path and decodes every segment exactly once.
func (t *PathTemplate) MatchEscapedPath(escapedPath string) (map[string]string, bool, error) {
	if t == nil {
		return nil, false, fmt.Errorf("route path template is nil")
	}
	if escapedPath == "" || escapedPath[0] != '/' || strings.ContainsAny(escapedPath, "?#") {
		return nil, false, fmt.Errorf("escaped route path must be absolute and contain no query or fragment")
	}
	parts := splitPath(escapedPath)
	if len(parts) != len(t.segments) {
		return nil, false, nil
	}
	params := make(map[string]string, len(t.parameters))
	for i, segment := range t.segments {
		value, err := url.PathUnescape(parts[i])
		if err != nil {
			return nil, false, fmt.Errorf("decode route path segment %d: %w", i, err)
		}
		if segment.placeholder {
			params[segment.value] = value
			continue
		}
		if segment.value != value {
			return nil, false, nil
		}
	}
	return params, true, nil
}

func (t *PathTemplate) shape() string {
	if t == nil {
		return ""
	}
	parts := make([]string, len(t.segments))
	for i, segment := range t.segments {
		parts[i] = segment.value
		if segment.placeholder {
			parts[i] = "{}"
		}
	}
	return "/" + strings.Join(parts, "/")
}

func splitPath(path string) []string {
	trimmed := strings.Trim(path, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

func isPlaceholderName(name string) bool {
	for i, char := range name {
		if i == 0 {
			if char != '_' && (char < 'A' || char > 'Z') && (char < 'a' || char > 'z') {
				return false
			}
			continue
		}
		if char != '_' && (char < 'A' || char > 'Z') && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return false
		}
	}
	return name != ""
}
