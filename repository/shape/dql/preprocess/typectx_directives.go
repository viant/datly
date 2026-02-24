package preprocess

import (
	"regexp"
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
)

var (
	packageLinePattern = regexp.MustCompile(`(?i)^\s*#package\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*$`)
	importLinePattern  = regexp.MustCompile(`(?i)^\s*#import\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*$`)
)

func parseTypeContextDirective(line, fullDQL string, offset int, ctx *typectx.Context) []*dqlshape.Diagnostic {
	var diagnostics []*dqlshape.Diagnostic
	if pkg, ok := parsePackageLineDirective(line); ok {
		ctx.DefaultPackage = pkg
		return nil
	}
	if alias, pkg, ok := parseImportLineDirective(line); ok {
		ctx.Imports = append(ctx.Imports, typectx.Import{Alias: alias, Package: pkg})
		return nil
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(line)), "#package") {
		diagnostics = append(diagnostics, directiveDiagnostic(
			dqldiag.CodeDirPackage,
			"invalid #package directive",
			"expected: #package('module/path')",
			fullDQL,
			offset,
		))
		return diagnostics
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(line)), "#import") {
		diagnostics = append(diagnostics, directiveDiagnostic(
			dqldiag.CodeDirImport,
			"invalid #import directive",
			"expected: #import('alias','github.com/acme/pkg')",
			fullDQL,
			offset,
		))
	}
	return diagnostics
}

func parsePackageLineDirective(line string) (string, bool) {
	matches := packageLinePattern.FindStringSubmatch(line)
	if len(matches) != 2 {
		return "", false
	}
	value := strings.TrimSpace(matches[1])
	if value == "" {
		return "", false
	}
	return value, true
}

func parseImportLineDirective(line string) (string, string, bool) {
	matches := importLinePattern.FindStringSubmatch(line)
	if len(matches) != 3 {
		return "", "", false
	}
	alias := strings.TrimSpace(matches[1])
	pkg := strings.TrimSpace(matches[2])
	if alias == "" || pkg == "" {
		return "", "", false
	}
	return alias, pkg, true
}

func isTypeContextDirectiveLine(line string) bool {
	line = strings.TrimSpace(line)
	switch {
	case strings.HasPrefix(line, "#package("), strings.HasPrefix(line, "#package ("):
		return true
	case strings.HasPrefix(line, "#import("), strings.HasPrefix(line, "#import ("):
		return true
	default:
		return false
	}
}
