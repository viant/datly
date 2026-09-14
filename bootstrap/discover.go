// Package bootstrap owns package-authority component discovery and artifact
// composition. It scans Go packages for tagged xdatly component holders and
// resolves them into canonical component metadata without registration, HTTP
// exposure, or dynamic type loading.
package bootstrap

import (
	"context"
	"path/filepath"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	xmodule "github.com/viant/x/module"
)

// RouteSource describes one discovered Component[I, O] holder field and carries
// the package-authority metadata needed to resolve its canonical component.
type RouteSource struct {
	// HolderType is the struct type declaring the component field.
	HolderType string
	// FieldName is the tagged holder field name — the component identity anchor.
	FieldName string
	// PackageName is the declaring file's package clause name.
	PackageName string
	// PackagePath is the module-qualified import path of the holder's package.
	PackagePath string
	// Dir is the absolute directory of the holder's package.
	Dir string
	// SourceFile is the absolute path of the declaring source file.
	SourceFile string
	// Tag is the complete typed component metadata from the holder field.
	Tag dtag.Component
	// InputType is the component input contract type name, rendered from the
	// first Component generic argument.
	InputType string
	// OutputType is the component output contract type name, rendered from the
	// second Component generic argument.
	OutputType string
	// Imports contains file-local aliases referenced by the input or output
	// contract expressions. Unrelated holder-file imports are not retained.
	Imports []spec.ImportSpec
	ordinal int
}

// DiscoverComponentsFromPackages is the default package-authority bootstrap
// discovery: it scans the Go packages under baseDir whose module-qualified
// import paths match the include patterns (and no exclude pattern) and returns
// one RouteSource per tagged Component[I, O] holder field.
//
// Patterns are matched against import paths: a pattern ending in "..." matches
// its prefix recursively (e.g. "example.com/app/svc/..."), any other pattern
// must match the import path exactly (path.Match wildcards permitted).
// Include patterns are required — an empty include is an error rather than a
// silent repo-wide scan. _test.go files and vendor/testdata/hidden directories
// are skipped. A component-tagged field that is malformed, incomplete, or not a
// Component[I, O] holder is a hard error.
func DiscoverComponentsFromPackages(ctx context.Context, baseDir string, include []string, exclude []string) ([]*RouteSource, error) {
	return (PackageDiscovery{BaseDir: baseDir, Include: include, Exclude: exclude}).Discover(ctx)
}

// PackageDiscovery selects package source roots independently of runtime exposure.
// ModuleDirs adds explicit local modules; imports are loaded separately as types.
type PackageDiscovery struct {
	BaseDir          string
	ModuleDirs       []string
	Include, Exclude []string
	Workspace        *xmodule.Workspace
}

func (d PackageDiscovery) Discover(ctx context.Context) ([]*RouteSource, error) {
	workspace := d.Workspace
	if workspace == nil {
		var err error
		workspace, err = (xmodule.LocalWorkspace{BaseDir: d.BaseDir, ModuleDirs: d.ModuleDirs}).Resolve(ctx)
		if err != nil {
			return nil, err
		}
	}
	var sources []*RouteSource
	walkErr := workspace.Walk(ctx, d.Include, d.Exclude, func(file xmodule.File) error {
		name := filepath.Base(file.Path)
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		holders, err := scanFileHolders(file.Path)
		if err != nil {
			return err
		}
		for ordinal, field := range holders.fields {
			sources = append(sources, &RouteSource{
				HolderType:  field.holderType,
				FieldName:   field.fieldName,
				PackageName: holders.packageName,
				PackagePath: file.ImportPath,
				Dir:         file.Dir,
				SourceFile:  file.Path,
				Tag:         field.tag,
				InputType:   field.inputType,
				OutputType:  field.outputType,
				Imports:     append([]spec.ImportSpec(nil), field.imports...),
				ordinal:     ordinal,
			})
		}
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	sort.SliceStable(sources, func(i, j int) bool {
		if sources[i].PackagePath != sources[j].PackagePath {
			return sources[i].PackagePath < sources[j].PackagePath
		}
		if sources[i].SourceFile != sources[j].SourceFile {
			return sources[i].SourceFile < sources[j].SourceFile
		}
		if sources[i].ordinal != sources[j].ordinal {
			return sources[i].ordinal < sources[j].ordinal
		}
		return sources[i].FieldName < sources[j].FieldName
	})
	return sources, nil
}
