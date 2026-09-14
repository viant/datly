package generate

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/viant/datly/spec"
	"github.com/viant/x"
)

func resolveParamDescriptor(param *spec.Parameter, lookup func(string) (*x.Type, error)) (*x.Type, error) {
	typeName := strings.TrimSpace(param.TypeExpr)
	if typeName == "" {
		typeName = strings.TrimSpace(param.OutputTypeExpr)
	}
	if typeName == "" {
		typeName = defaultTagTypeName(param.Tag)
	}
	if unwrapQualifiedTypeName(typeName) == "" {
		return nil, nil
	}
	descriptor, err := lookup(typeName)
	if err != nil {
		return nil, fmt.Errorf("resolve type %q: %w", typeName, err)
	}
	return descriptor, nil
}

func packageAlias(pkg string) string {
	pkg = strings.TrimSpace(pkg)
	if pkg == "" {
		return ""
	}
	if index := strings.LastIndex(pkg, "/"); index != -1 {
		pkg = pkg[index+1:]
	}
	var result strings.Builder
	for index, char := range pkg {
		if index == 0 && unicode.IsDigit(char) {
			result.WriteByte('_')
			result.WriteRune(char)
			continue
		}
		if unicode.IsLetter(char) || char == '_' || index > 0 && unicode.IsDigit(char) {
			result.WriteRune(char)
			continue
		}
		result.WriteByte('_')
	}
	alias := result.String()
	if alias == "" || strings.Trim(alias, "_") == "" || tokenKeyword(alias) {
		return "pkg"
	}
	return alias
}

func applyQualifiedWrapper(original, qualified string) string {
	trimmed := strings.TrimSpace(original)
	prefix := ""
	for {
		switch {
		case strings.HasPrefix(trimmed, "[]"):
			prefix += "[]"
			trimmed = strings.TrimSpace(trimmed[2:])
		case strings.HasPrefix(trimmed, "*"):
			prefix += "*"
			trimmed = strings.TrimSpace(trimmed[1:])
		default:
			return prefix + qualified
		}
	}
}

func ensureImport(plan *Plan, alias, pkg string) {
	if plan == nil || alias == "" || pkg == "" {
		return
	}
	for _, item := range plan.Imports {
		if item.Alias == alias && item.Package == pkg {
			return
		}
	}
	plan.Imports = append(plan.Imports, spec.ImportSpec{Alias: alias, Package: pkg})
}

func uniqueImportAlias(plan *Plan, pkg string) string {
	base := packageAlias(pkg)
	if plan == nil {
		if base == "error" {
			return "error2"
		}
		return base
	}
	hasImplicitImport := false
	for _, item := range plan.Imports {
		if strings.TrimSpace(item.Package) == strings.TrimSpace(pkg) {
			if alias := strings.TrimSpace(item.Alias); alias != "" {
				return alias
			}
			hasImplicitImport = true
		}
	}
	if hasImplicitImport && base != "error" {
		return base
	}
	used := map[string]bool{"error": true}
	for _, item := range plan.Imports {
		alias := strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = packageAlias(item.Package)
		}
		used[alias] = true
	}
	if !used[base] {
		return base
	}
	for suffix := 2; ; suffix++ {
		candidate := base + strconv.Itoa(suffix)
		if !used[candidate] {
			return candidate
		}
	}
}

func tokenKeyword(value string) bool {
	switch value {
	case "break", "default", "func", "interface", "select", "case", "defer", "go", "map", "struct",
		"chan", "else", "goto", "package", "switch", "const", "fallthrough", "if", "range", "type",
		"continue", "for", "import", "return", "var":
		return true
	default:
		return false
	}
}
