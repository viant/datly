package codegen

import "strings"

func (s Selector) Name() string {
	return strings.Join(s, "")
}

func (s Selector) Leaf() Selector {
	return s[len(s)-1:]
}

func (s Selector) Resolve(prefix string, loc ...string) string {
	result := append([]string{prefix}, s...)
	return Selector(result).Absolute(loc...)
}
func (s Selector) Absolute(loc ...string) string {
	if len(s) < 1 {
		return strings.Join(loc, ".")
	}
	parts := s
	if len(loc) > 0 {
		parts = append(parts, loc...)
	}
	return strings.Join(parts, ".")
}

func (s Selector) Path(loc ...string) string {
	if len(s) <= 1 {
		return "/" + strings.Join(loc, "/")
	}
	parts := s[1:]
	if len(loc) > 0 {
		parts = append(parts, loc...)
	}
	return "/" + strings.Join(parts, "/")
}
