package spec

import (
	"fmt"
	"strings"
)

type Kind string

const (
	KindComponent Kind = "component"
	KindView      Kind = "view"
)

type Key struct {
	Kind  Kind   `json:"kind"`
	Scope string `json:"scope"`
	Name  string `json:"name"`
}

func (k Key) String() string {
	return fmt.Sprintf("%s:%s:%s", k.Kind, escapeKeyPart(k.Scope), escapeKeyPart(k.Name))
}

func ParseKey(input string) (Key, error) {
	parts := splitEscaped(input)
	if len(parts) != 3 {
		return Key{}, fmt.Errorf("invalid key %q", input)
	}
	return Key{
		Kind:  Kind(parts[0]),
		Scope: unescapeKeyPart(parts[1]),
		Name:  unescapeKeyPart(parts[2]),
	}, nil
}

func escapeKeyPart(input string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `:`, `\:`)
	return replacer.Replace(input)
}

func unescapeKeyPart(input string) string {
	var b strings.Builder
	b.Grow(len(input))
	escaped := false
	for _, r := range input {
		if escaped {
			b.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		b.WriteRune(r)
	}
	if escaped {
		b.WriteRune('\\')
	}
	return b.String()
}

func splitEscaped(input string) []string {
	var result []string
	var b strings.Builder
	escaped := false
	for _, r := range input {
		if escaped {
			b.WriteRune(r)
			escaped = false
			continue
		}
		switch r {
		case '\\':
			escaped = true
		case ':':
			result = append(result, b.String())
			b.Reset()
		default:
			b.WriteRune(r)
		}
	}
	result = append(result, b.String())
	return result
}
