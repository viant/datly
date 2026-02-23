package compile

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/typectx"
	"golang.org/x/mod/modfile"
)

func applyTypeContextDefaults(ctx *typectx.Context, source *shape.Source, opts *shape.CompileOptions, layout compilePathLayout) *typectx.Context {
	ret := cloneTypeContext(ctx)
	if shouldInferTypeContext(opts) {
		ret = mergeTypeContext(ret, inferDatlyGenTypeContext(source, layout))
	}
	if opts != nil {
		ret = ensureTypeContext(ret)
		if ret != nil {
			if value := strings.TrimSpace(opts.TypePackageDir); value != "" {
				ret.PackageDir = value
			}
			if value := strings.TrimSpace(opts.TypePackageName); value != "" {
				ret.PackageName = value
			}
			if value := strings.TrimSpace(opts.TypePackagePath); value != "" {
				ret.PackagePath = value
			}
		}
	}
	return normalizeTypeContext(ret)
}

func shouldInferTypeContext(opts *shape.CompileOptions) bool {
	if opts == nil || opts.InferTypeContext == nil {
		return true
	}
	return *opts.InferTypeContext
}

func mergeTypeContext(dst *typectx.Context, src *typectx.Context) *typectx.Context {
	if src == nil {
		return dst
	}
	dst = ensureTypeContext(dst)
	if strings.TrimSpace(dst.DefaultPackage) == "" {
		dst.DefaultPackage = strings.TrimSpace(src.DefaultPackage)
	}
	if len(dst.Imports) == 0 && len(src.Imports) > 0 {
		dst.Imports = append([]typectx.Import{}, src.Imports...)
	}
	if strings.TrimSpace(dst.PackageDir) == "" {
		dst.PackageDir = strings.TrimSpace(src.PackageDir)
	}
	if strings.TrimSpace(dst.PackageName) == "" {
		dst.PackageName = strings.TrimSpace(src.PackageName)
	}
	if strings.TrimSpace(dst.PackagePath) == "" {
		dst.PackagePath = strings.TrimSpace(src.PackagePath)
	}
	return dst
}

func inferDatlyGenTypeContext(source *shape.Source, layout compilePathLayout) *typectx.Context {
	if source == nil {
		return nil
	}
	sourcePath := strings.TrimSpace(source.Path)
	if sourcePath == "" {
		return nil
	}
	normalizedPath := filepath.ToSlash(filepath.Clean(sourcePath))
	idx := strings.Index(normalizedPath, layout.dqlMarker)
	if idx == -1 {
		return nil
	}
	projectRoot := filepath.FromSlash(strings.TrimSuffix(normalizedPath[:idx], "/"))
	rel := strings.TrimPrefix(normalizedPath[idx+len(layout.dqlMarker):], "/")
	if rel == "" {
		return nil
	}
	routeDir := strings.Trim(path.Dir(rel), "/")
	if routeDir == "." {
		routeDir = ""
	}
	packageDir := "pkg"
	if routeDir != "" {
		packageDir = path.Join(packageDir, routeDir)
	}
	packageName := "main"
	if routeDir != "" {
		packageName = path.Base(routeDir)
	}
	packagePath := ""
	if module := detectModulePath(projectRoot); module != "" {
		packagePath = path.Join(module, packageDir)
	}
	return normalizeTypeContext(&typectx.Context{
		PackageDir:  packageDir,
		PackageName: packageName,
		PackagePath: packagePath,
	})
}

func detectModulePath(projectRoot string) string {
	if strings.TrimSpace(projectRoot) == "" {
		return ""
	}
	goModPath := filepath.Join(projectRoot, "go.mod")
	data, err := os.ReadFile(goModPath)
	if err != nil {
		return ""
	}
	parsed, err := modfile.Parse(goModPath, data, nil)
	if err != nil || parsed == nil || parsed.Module == nil {
		return ""
	}
	return strings.TrimSpace(parsed.Module.Mod.Path)
}

func ensureTypeContext(ctx *typectx.Context) *typectx.Context {
	if ctx != nil {
		return ctx
	}
	return &typectx.Context{}
}

func cloneTypeContext(ctx *typectx.Context) *typectx.Context {
	if ctx == nil {
		return nil
	}
	ret := &typectx.Context{
		DefaultPackage: strings.TrimSpace(ctx.DefaultPackage),
		PackageDir:     strings.TrimSpace(ctx.PackageDir),
		PackageName:    strings.TrimSpace(ctx.PackageName),
		PackagePath:    strings.TrimSpace(ctx.PackagePath),
	}
	if len(ctx.Imports) > 0 {
		ret.Imports = append([]typectx.Import{}, ctx.Imports...)
	}
	return ret
}

func normalizeTypeContext(ctx *typectx.Context) *typectx.Context {
	if ctx == nil {
		return nil
	}
	if strings.TrimSpace(ctx.DefaultPackage) == "" &&
		len(ctx.Imports) == 0 &&
		strings.TrimSpace(ctx.PackageDir) == "" &&
		strings.TrimSpace(ctx.PackageName) == "" &&
		strings.TrimSpace(ctx.PackagePath) == "" {
		return nil
	}
	return ctx
}
