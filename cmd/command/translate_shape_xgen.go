package command

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	dqlir "github.com/viant/datly/repository/shape/dql/ir"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/repository/shape/xgen"
	"gopkg.in/yaml.v3"
)

// generateShapeTypes emits a Go type file (shapes_gen.go) for the compiled
// component. It is a best-effort step: any failure is logged as a warning so
// the route YAML is still written successfully.
//
// Normal flow  — types already exist in pkg/shapes_gen.go:
//
//	xgen merges the file, updating only the types produced by this DQL.
//
// Backfill flow — no types file yet:
//
//	xgen generates stub types from the statically-inferred columns
//	(explicit SELECT columns give accurate field names/types; SELECT *
//	produces a minimal stub that the user should refine or regenerate
//	after DB discovery).
func generateShapeTypes(sourceAbsPath string, payload *shapeRuleFile, component *shapeLoad.Component) {
	if component == nil || component.TypeContext == nil {
		return
	}
	ctx := component.TypeContext
	if strings.TrimSpace(ctx.PackageDir) == "" {
		return
	}

	projectDir := findProjectDir(sourceAbsPath)
	if projectDir == "" {
		fmt.Printf("WARNING: shape xgen: cannot locate go.mod from %s, skipping type generation\n", sourceAbsPath)
		return
	}

	packageDir := strings.TrimSpace(ctx.PackageDir)
	if !filepath.IsAbs(packageDir) {
		packageDir = filepath.Join(projectDir, packageDir)
	}

	data, err := yaml.Marshal(payload)
	if err != nil {
		fmt.Printf("WARNING: shape xgen: marshal failed for %s: %v\n", sourceAbsPath, err)
		return
	}
	doc, err := dqlir.FromYAML(data)
	if err != nil {
		fmt.Printf("WARNING: shape xgen: IR parse failed for %s: %v\n", sourceAbsPath, err)
		return
	}

	shapeDoc := buildShapeDocument(doc, ctx)
	cfg := &xgen.Config{
		ProjectDir:  projectDir,
		PackageDir:  packageDir,
		PackageName: strings.TrimSpace(ctx.PackageName),
		PackagePath: strings.TrimSpace(ctx.PackagePath),
	}

	result, err := xgen.GenerateFromDQLShape(shapeDoc, cfg)
	if err != nil {
		fmt.Printf("WARNING: shape xgen: type generation skipped for %s: %v\n", filepath.Base(sourceAbsPath), err)
		return
	}
	fmt.Printf("generated types %s → %s\n", strings.Join(result.Types, ", "), result.FilePath)
}

// buildShapeDocument bridges an ir.Document into the shape.Document expected by xgen.
func buildShapeDocument(doc *dqlir.Document, ctx *typectx.Context) *dqlshape.Document {
	return &dqlshape.Document{
		Root:        doc.Root,
		TypeContext: ctx,
	}
}

// findProjectDir walks up from sourcePath until it finds a directory containing
// go.mod, returning that directory. Returns "" when no go.mod is found.
func findProjectDir(sourcePath string) string {
	dir := filepath.Dir(filepath.Clean(sourcePath))
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
