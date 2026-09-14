package report

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	generate "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
)

func TestGeneratedReportInputEmitsAsNamedTranscribedType(t *testing.T) {
	project, catalog := generatedProject(t, &spec.ReportSettings{Enabled: true})
	derived := project.Derived()[0]
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{
		PackagePath: derived.Type.PkgPath, DefaultPackage: derived.Type.PkgPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	packageDir := filepath.Join(root, "reporting")
	emitter := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: derived.Type.PkgPath, Name: "SpendSource"}, Name: "SpendSource",
		Routes: []*spec.Route{{Method: "GET", Path: "/spend"}},
	}
	result, err := generate.New(generate.Input{
		Component: emitter, TargetPackage: derived.Type.PkgPath, TypeResolver: resolver,
		GeneratedTypes: []generate.GeneratedTypeReference{{DescriptorKey: derived.Type.Key()}},
	}).Generate(packageDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plan.GeneratedTypes) != 1 || result.Plan.GeneratedTypes[0].Name != "SpendCubeInput" {
		t.Fatalf("generated report type plan = %+v", result.Plan.GeneratedTypes)
	}
	content, err := os.ReadFile(filepath.Join(packageDir, "spend_cube_input.go"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(content)
	normalized := strings.Join(strings.Fields(text), " ")
	for _, expected := range []string{
		"type SpendCubeInput struct", "Dimensions struct", "Measures struct", "Filters struct", "AccountIDs *[]int",
		`json:"accountIDs,omitempty"`, "OrderBy []string", "Limit *int", "Offset *int",
	} {
		if !strings.Contains(normalized, expected) {
			t.Fatalf("generated report input is missing %q:\n%s", expected, text)
		}
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated report package does not compile: %v\n%s", runErr, output)
	}
	loaded, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "reporting")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, declared := range loaded.Types {
		if declared != nil && declared.Name == "SpendCubeInput" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("loaded package has no named SpendCubeInput: %+v", loaded.Types)
	}
}
