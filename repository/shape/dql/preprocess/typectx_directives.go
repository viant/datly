package preprocess

import (
	"strings"

	dqldiag "github.com/viant/datly/repository/shape/dql/diag"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
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
	args, ok := parseExactHashDirectiveCall(line, "package")
	if !ok || len(args) != 1 {
		return "", false
	}
	value, ok := parseQuotedLiteral(args[0])
	if !ok {
		return "", false
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	return value, true
}

func parseImportLineDirective(line string) (string, string, bool) {
	args, ok := parseExactHashDirectiveCall(line, "import")
	if !ok || len(args) != 2 {
		return "", "", false
	}
	alias, ok := parseQuotedLiteral(args[0])
	if !ok {
		return "", "", false
	}
	pkg, ok := parseQuotedLiteral(args[1])
	if !ok {
		return "", "", false
	}
	alias = strings.TrimSpace(alias)
	pkg = strings.TrimSpace(pkg)
	if alias == "" || pkg == "" {
		return "", "", false
	}
	return alias, pkg, true
}

func isTypeContextDirectiveLine(line string) bool {
	line = strings.ToLower(strings.TrimSpace(line))
	switch {
	case strings.HasPrefix(line, "#package("), strings.HasPrefix(line, "#package ("):
		return true
	case strings.HasPrefix(line, "#import("), strings.HasPrefix(line, "#import ("):
		return true
	default:
		return false
	}
}

func parseExactHashDirectiveCall(line, directive string) ([]string, bool) {
	input := strings.TrimSpace(line)
	if input == "" || input[0] != '#' {
		return nil, false
	}
	index := skipSpaces(input, 1)
	start := index
	for index < len(input) && isIdentifierPart(input[index]) {
		index++
	}
	if start == index {
		return nil, false
	}
	if !strings.EqualFold(input[start:index], directive) {
		return nil, false
	}
	index = skipSpaces(input, index)
	if index >= len(input) || input[index] != '(' {
		return nil, false
	}
	body, end, ok := readCallBody(input, index)
	if !ok {
		return nil, false
	}
	index = skipSpaces(input, end+1)
	if index != len(input) {
		return nil, false
	}
	return splitCallArgs(body), true
}
