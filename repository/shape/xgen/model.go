package xgen

import "github.com/viant/x"

type (
	ViewTypeContext struct {
		ViewName   string
		SchemaName string
	}

	RouteTypeContext struct {
		RouteName string
		RouteURI  string
		RouteRef  string
		TypeName  string
	}
)

// Config controls shape->Go generation.
type Config struct {
	// ProjectDir points to target Go project root.
	ProjectDir string
	// PackageDir points to package directory inside the project (relative or absolute).
	PackageDir string
	// PackageName sets generated package name; defaults to basename(PackageDir).
	PackageName string
	// PackagePath sets fully-qualified import path; when empty it's derived from go.mod + PackageDir.
	PackagePath string
	// FileName sets generated filename; defaults to shapes_gen.go.
	FileName string
	// TypePrefix prefixes generated type names.
	TypePrefix string
	// ViewSuffix appends suffix to generated view type names when schema name is absent.
	ViewSuffix string
	// InputSuffix appends suffix to generated route input type names when explicit type name is absent.
	InputSuffix string
	// OutputSuffix appends suffix to generated route output type names when explicit type name is absent.
	OutputSuffix string
	// ViewTypeNamer customizes final view type name.
	ViewTypeNamer func(ctx ViewTypeContext) string
	// InputTypeNamer customizes final input type name.
	InputTypeNamer func(ctx RouteTypeContext) string
	// OutputTypeNamer customizes final output type name.
	OutputTypeNamer func(ctx RouteTypeContext) string
	// Registry allows reusing an external viant/x registry.
	Registry *x.Registry
	// AllowUnsafeRewrite allows overwriting existing generated files even when
	// type provenance indicates unresolved/unsafe origins. Default false.
	AllowUnsafeRewrite bool
	// AllowedProvenanceKinds controls which provenance kinds are trusted for updates.
	// Defaults to builtin, resource_type and ast_type.
	AllowedProvenanceKinds []string
	// AllowedSourceRoots controls additional trusted roots for provenance files.
	// ProjectDir is always implicitly trusted.
	AllowedSourceRoots []string
	// UseGoModuleResolve enables go.mod + replace-based source resolution. Default true.
	UseGoModuleResolve *bool
	// UseGOPATHFallback enables GOPATH/src fallback when go.mod resolution misses. Default true.
	UseGOPATHFallback *bool
	// StrictProvenance blocks updates on policy violations. Default true.
	StrictProvenance *bool
}

// Result captures generation outputs.
type Result struct {
	FilePath    string
	PackagePath string
	PackageName string
	Types       []string
}
