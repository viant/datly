package route

import (
	"fmt"
	"path"
	"strings"
)

// Exposure selects public package import paths independently of dependency loading.
// An empty include list exposes nothing. Exclusions take precedence.
type Exposure struct{ include, exclude []string }

func NewExposure(include, exclude []string) (*Exposure, error) {
	p := &Exposure{include: append([]string(nil), include...), exclude: append([]string(nil), exclude...)}
	for _, patterns := range [][]string{p.include, p.exclude} {
		for i, pattern := range patterns {
			pattern = strings.TrimSpace(pattern)
			if pattern == "" {
				return nil, fmt.Errorf("exposure package pattern must not be empty")
			}
			if _, err := path.Match(strings.TrimSuffix(pattern, "/..."), ""); err != nil {
				return nil, fmt.Errorf("invalid exposure pattern %q: %w", pattern, err)
			}
			patterns[i] = pattern
		}
	}
	return p, nil
}

func (p *Exposure) Allows(packagePath string) bool {
	if p == nil {
		return true
	}
	return p.matches(p.include, packagePath) && !p.matches(p.exclude, packagePath)
}

func (*Exposure) matches(patterns []string, packagePath string) bool {
	for _, pattern := range patterns {
		if pattern == "..." {
			return true
		}
		if strings.HasSuffix(pattern, "/...") {
			prefix := strings.TrimSuffix(pattern, "/...")
			if packagePath == prefix || strings.HasPrefix(packagePath, prefix+"/") {
				return true
			}
			continue
		}
		if match, _ := path.Match(pattern, packagePath); match {
			return true
		}
	}
	return false
}
