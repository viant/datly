package typecatalog

import (
	"path"
	"strings"
)

type ContextValidationIssue struct {
	Field   string
	Message string
}

// NormalizeContext returns a detached, canonical type-resolution context.
func NormalizeContext(input *ResolutionContext) *ResolutionContext {
	if input == nil {
		return nil
	}
	result := &ResolutionContext{
		DefaultPackage: strings.TrimSpace(input.DefaultPackage),
		PackageDir:     cleanDirectory(strings.TrimSpace(input.PackageDir)),
		PackageName:    strings.TrimSpace(input.PackageName),
		PackagePath:    cleanSlashes(strings.TrimSpace(input.PackagePath)),
	}
	if result.PackageName == "" {
		if result.PackagePath != "" {
			result.PackageName = path.Base(result.PackagePath)
		} else if result.PackageDir != "" {
			result.PackageName = path.Base(result.PackageDir)
		}
	}
	if result.DefaultPackage == "" && result.PackagePath != "" {
		result.DefaultPackage = result.PackagePath
	}
	for _, item := range input.Imports {
		packagePath := cleanSlashes(strings.TrimSpace(item.Package))
		if packagePath == "" {
			continue
		}
		alias := strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = path.Base(packagePath)
		}
		result.Imports = append(result.Imports, PackageImport{Alias: alias, Package: packagePath})
	}
	if result.DefaultPackage == "" && len(result.Imports) == 0 &&
		result.PackageDir == "" && result.PackageName == "" && result.PackagePath == "" {
		return nil
	}
	return result
}

func ValidateContext(context *ResolutionContext) []ContextValidationIssue {
	context = NormalizeContext(context)
	if context == nil {
		return nil
	}
	var result []ContextValidationIssue
	if strings.Contains(context.PackageName, "/") {
		result = append(result, ContextValidationIssue{Field: "PackageName", Message: "package name must not contain path separators"})
	}
	if context.PackagePath != "" && strings.Contains(context.PackagePath, ".") {
		base := path.Base(context.PackagePath)
		if context.PackageName != "" && base != context.PackageName {
			result = append(result, ContextValidationIssue{Field: "PackagePath", Message: "package path basename differs from package name"})
		}
	}
	return result
}

func cleanSlashes(value string) string {
	value = strings.ReplaceAll(value, "\\", "/")
	value = strings.TrimSpace(value)
	return strings.Trim(value, "/")
}

func cleanDirectory(value string) string {
	value = strings.ReplaceAll(value, "\\", "/")
	return strings.TrimSpace(value)
}
