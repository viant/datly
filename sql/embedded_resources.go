package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/toolbox/data"
	"io/fs"
	"path"
	"strings"
)

// ExpandEmbeddedResources materializes authored resource arguments before SQL
// parsing. Each occurrence owns its arguments; filesystems are never modified.
func ExpandEmbeddedResources(ctx context.Context, text string, resources fs.FS) (string, error) {
	return expandEmbeddedResources(ctx, text, resources, "", map[string]bool{}, 0)
}

func expandEmbeddedResources(ctx context.Context, text string, resources fs.FS, base string, active map[string]bool, depth int) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if depth > 128 {
		return "", fmt.Errorf("SQL resource nesting exceeds 128")
	}
	var result strings.Builder
	for {
		index := strings.Index(text, "${embed")
		if index < 0 {
			result.WriteString(text)
			return result.String(), nil
		}
		result.WriteString(text[:index])
		fragment := text[index:]
		offset := len("${embed")
		variables := data.NewMap()
		if strings.HasPrefix(fragment[offset:], "(") {
			decoder := json.NewDecoder(strings.NewReader(fragment[offset+1:]))
			var args map[string]any
			if err := decoder.Decode(&args); err != nil {
				return "", fmt.Errorf("SQL embed arguments: %w", err)
			}
			if args == nil {
				return "", fmt.Errorf("SQL embed arguments must be an object")
			}
			for k, v := range args {
				variables[k] = v
			}
			offset += 1 + int(decoder.InputOffset())
			for offset < len(fragment) && (fragment[offset] == ' ' || fragment[offset] == '\n' || fragment[offset] == '\t' || fragment[offset] == '\r') {
				offset++
			}
			if offset >= len(fragment) || fragment[offset] != ')' {
				return "", fmt.Errorf("SQL embed arguments require closing parenthesis")
			}
			offset++
		}
		if offset >= len(fragment) || fragment[offset] != ':' {
			return "", fmt.Errorf("SQL embed requires a resource path")
		}
		offset++
		end := strings.IndexByte(fragment[offset:], '}')
		if end < 0 {
			return "", fmt.Errorf("SQL embed requires closing brace")
		}
		end += offset
		reference := strings.TrimSpace(fragment[offset:end])
		if reference == "" {
			return "", fmt.Errorf("SQL embed resource path is empty")
		}
		resolved := embeddedResourcePath(base, reference)
		if active[resolved] {
			return "", fmt.Errorf("SQL resource cycle at %q", resolved)
		}
		if resources == nil {
			return "", fmt.Errorf("SQL resource filesystem is required")
		}
		body, err := fs.ReadFile(resources, resolved)
		if err != nil {
			return "", fmt.Errorf("read SQL embed %q: %w", resolved, err)
		}
		active[resolved] = true
		expanded, err := expandEmbeddedResources(ctx, string(body), resources, resolved, active, depth+1)
		delete(active, resolved)
		if err != nil {
			return "", err
		}
		// Match the original loader's inner-first substitution order. A nested
		// occurrence may override a name without contaminating its siblings.
		expanded = variables.ExpandAsText(expanded)
		for k, v := range variables {
			expanded = strings.ReplaceAll(expanded, "${"+k+"}", fmt.Sprint(v))
		}
		result.WriteString(expanded)
		text = fragment[end+1:]
	}
}

func embeddedResourcePath(base, reference string) string {
	if strings.Contains(reference, ":") {
		return reference
	}
	namespace, relative, qualified := strings.Cut(base, ":")
	if !qualified {
		relative = base
		namespace = ""
	}
	joined := path.Join(path.Dir(relative), reference)
	if namespace != "" {
		return namespace + ":" + joined
	}
	return joined
}
