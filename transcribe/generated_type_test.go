package transcribe

import (
	"context"
	"go/ast"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	generate "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

func TestProjectGenerationCarriesGeneratedTypeReferences(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	compiled := compileProjectSource(t, "Spend", "/spend", "SELECT account_id FROM spend")
	key, err := compiled.projectKey()
	if err != nil {
		t.Fatal(err)
	}
	packageDir := filepath.ToSlash(filepath.Join("generated", projectComponentSlug(key)))
	targetPackage := "example.com/generated/" + packageDir
	limitField := &ast.Field{
		Names: []*ast.Ident{ast.NewIdent("Limit")}, Type: &ast.StarExpr{X: ast.NewIdent("int")},
	}
	descriptor := &x.Type{
		PkgPath: targetPackage, Name: "SpendCubeInput",
		SynteticType: &smodel.Type{
			PkgPath: targetPackage, Name: "SpendCubeInput",
			TypeSpec: &ast.TypeSpec{Name: ast.NewIdent("SpendCubeInput"), Type: &ast.StructType{Fields: &ast.FieldList{List: []*ast.Field{limitField}}}},
		},
	}
	compiled.Source.Types = typecatalog.NewCatalog()
	if err = compiled.Source.Types.Register(typecatalog.TypeOriginGenerated, descriptor); err != nil {
		t.Fatal(err)
	}
	compiled.GeneratedTypes = []generate.GeneratedTypeReference{{DescriptorKey: descriptor.Key()}}
	project, err := (&ProjectGeneration{Components: []*Result{compiled}}).Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	entry := projectEntryByName(t, project.Manifest, "Spend")
	generatedPath := filepath.ToSlash(filepath.Join(entry.Package, "spend_cube_input.go"))
	if !containsProjectValue(entry.Artifacts, generatedPath) {
		t.Fatalf("generated type is absent from project artifacts: %+v", entry.Artifacts)
	}
	content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(generatedPath)))
	if err != nil || !strings.Contains(string(content), "type SpendCubeInput struct") {
		t.Fatalf("generated type content = %q, %v", content, err)
	}
	command := exec.Command("go", "vet", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated project does not compile: %v\n%s", runErr, output)
	}
}
