package compile

import (
	"path/filepath"
	"strings"

	"github.com/viant/datly/repository/shape"
)

type compilePathLayout struct {
	dqlMarker      string
	routesRelative string
}

func defaultCompilePathLayout() compilePathLayout {
	return compilePathLayout{
		dqlMarker:      "/dql/",
		routesRelative: "repo/dev/Datly/routes",
	}
}

func newCompilePathLayout(opts *shape.CompileOptions) compilePathLayout {
	ret := defaultCompilePathLayout()
	if opts == nil {
		return ret
	}
	if marker := normalizeDQLMarker(opts.DQLPathMarker); marker != "" {
		ret.dqlMarker = marker
	}
	if rel := normalizeRoutesRelative(opts.RoutesRelativePath); rel != "" {
		ret.routesRelative = rel
	}
	return ret
}

func normalizeDQLMarker(input string) string {
	input = strings.TrimSpace(strings.ReplaceAll(input, "\\", "/"))
	if input == "" {
		return ""
	}
	input = strings.Trim(input, "/")
	if input == "" {
		return ""
	}
	return "/" + input + "/"
}

func normalizeRoutesRelative(input string) string {
	input = strings.TrimSpace(strings.ReplaceAll(input, "\\", "/"))
	input = strings.Trim(input, "/")
	if input == "" {
		return ""
	}
	return input
}

func joinRelativePath(base string, rel string) string {
	rel = normalizeRoutesRelative(rel)
	if rel == "" {
		return base
	}
	parts := strings.Split(rel, "/")
	args := make([]string, 0, len(parts)+1)
	args = append(args, base)
	args = append(args, parts...)
	return filepath.Join(args...)
}
