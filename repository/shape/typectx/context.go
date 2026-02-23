package typectx

import (
	"path"
	"strings"
)

// ValidationIssue captures context consistency problems.
type ValidationIssue struct {
	Field   string
	Message string
}

// Normalize trims and canonicalizes context fields.
func Normalize(input *Context) *Context {
	if input == nil {
		return nil
	}
	ret := &Context{
		DefaultPackage: strings.TrimSpace(input.DefaultPackage),
		PackageDir:     cleanSlashes(strings.TrimSpace(input.PackageDir)),
		PackageName:    strings.TrimSpace(input.PackageName),
		PackagePath:    cleanSlashes(strings.TrimSpace(input.PackagePath)),
	}
	if ret.PackageName == "" {
		if ret.PackagePath != "" {
			ret.PackageName = path.Base(ret.PackagePath)
		} else if ret.PackageDir != "" {
			ret.PackageName = path.Base(ret.PackageDir)
		}
	}
	if ret.DefaultPackage == "" && ret.PackagePath != "" {
		ret.DefaultPackage = ret.PackagePath
	}
	for _, item := range input.Imports {
		pkg := cleanSlashes(strings.TrimSpace(item.Package))
		if pkg == "" {
			continue
		}
		alias := strings.TrimSpace(item.Alias)
		if alias == "" {
			alias = path.Base(pkg)
		}
		ret.Imports = append(ret.Imports, Import{
			Alias:   alias,
			Package: pkg,
		})
	}
	if ret.DefaultPackage == "" &&
		len(ret.Imports) == 0 &&
		ret.PackageDir == "" &&
		ret.PackageName == "" &&
		ret.PackagePath == "" {
		return nil
	}
	return ret
}

// Validate checks context consistency.
func Validate(ctx *Context) []ValidationIssue {
	ctx = Normalize(ctx)
	if ctx == nil {
		return nil
	}
	var result []ValidationIssue
	if strings.Contains(ctx.PackageName, "/") {
		result = append(result, ValidationIssue{
			Field:   "PackageName",
			Message: "package name must not contain path separators",
		})
	}
	if ctx.PackagePath != "" && strings.Contains(ctx.PackagePath, ".") {
		base := path.Base(ctx.PackagePath)
		if ctx.PackageName != "" && base != ctx.PackageName {
			result = append(result, ValidationIssue{
				Field:   "PackagePath",
				Message: "package path basename differs from package name",
			})
		}
	}
	return result
}

func cleanSlashes(value string) string {
	value = strings.ReplaceAll(value, "\\", "/")
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "/")
	return value
}
