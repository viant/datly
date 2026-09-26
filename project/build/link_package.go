package build

import (
	"fmt"
	"go/token"
	"path"
	"regexp"
	"strings"
)

const DefaultLinkPackage = "internal/dependencylink"

var linkPackageName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// resolveLinkPackage accepts a package name or an internal/<name> path. The
// link package must stay inside the application's internal directory.
func resolveLinkPackage(value string) (relative, name string, err error) {
	value = strings.TrimSpace(value)
	if value == "" {
		value = DefaultLinkPackage
	} else if !strings.Contains(value, "/") {
		value = "internal/" + value
	}
	if path.Clean(value) != value || strings.Count(value, "/") != 1 || !strings.HasPrefix(value, "internal/") {
		return "", "", fmt.Errorf("link package must be internal/<name>: %q", value)
	}
	name = strings.TrimPrefix(value, "internal/")
	if !linkPackageName.MatchString(name) || token.Lookup(name).IsKeyword() {
		return "", "", fmt.Errorf("invalid link package name %q", name)
	}
	return value, name, nil
}
