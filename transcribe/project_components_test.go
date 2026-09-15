package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/datly/transcribe/testdata/linkedcontract"
)

func TestProjectGenerationResolvesSiblingDQLComponent(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	sourceRoot := filepath.Join(t.TempDir(), "dql", "dev")
	child := compileProjectComponentAt(t, "UserAcl", filepath.Join(sourceRoot, "user_acl.dql"), `
#setting($_ = $route('/v1/api/dev/user-acl', 'GET'))
SELECT 1 AS ID`)
	parent := compileProjectComponentAt(t, "Vendors", filepath.Join(sourceRoot, "vendor", "vendors.dql"), `
#setting($_ = $route('/v1/api/dev/vendors', 'GET'))
#define($_ = $Auth<?>(component/../user_acl))
SELECT 1 AS ID`)

	project := &ProjectGeneration{Components: []*Result{parent, child}}
	prepared, err := project.prepare(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(prepared) != 2 || prepared[0].compiled.Component.Name != "UserAcl" || prepared[1].compiled.Component.Name != "Vendors" {
		t.Fatalf("dependency order = %+v", prepared)
	}
	param := spec.EffectiveParameters(prepared[1].compiled.Component.Parameters)[0]
	if param.Source.Name != "GET:/v1/api/dev/user-acl" || !strings.HasPrefix(param.TypeExpr, "*") ||
		!strings.HasSuffix(param.TypeExpr, ".UserAclOutput") {
		t.Fatalf("resolved component parameter = %+v", param)
	}
	if len(prepared[1].plan.Input.Fields) != 1 || !strings.HasSuffix(prepared[1].plan.Input.Fields[0].Type, ".UserAclOutput") {
		t.Fatalf("resolved parent input = %+v", prepared[1].plan.Input)
	}
	if !planImportsPackage(prepared[1].plan.Imports, prepared[0].targetPackage) {
		t.Fatalf("parent imports = %+v, child package = %q", prepared[1].plan.Imports, prepared[0].targetPackage)
	}

	generated, err := project.Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	parentEntry := projectEntryByName(t, generated.Manifest, "Vendors")
	input, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(parentEntry.Package), "input.go"))
	if err != nil || !strings.Contains(string(input), "kind=component") ||
		!strings.Contains(string(input), "in=GET:/v1/api/dev/user-acl") {
		t.Fatalf("generated parent input = %q, %v", input, err)
	}
}

func TestProjectGenerationResolvesPersistedSiblingComponent(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	sourceRoot := filepath.Join(t.TempDir(), "dql", "dev")
	child := compileProjectComponentAt(t, "UserAcl", filepath.Join(sourceRoot, "user_acl.dql"), `
#setting($_ = $route('/v1/api/dev/user-acl', 'GET'))
SELECT 1 AS ID`)
	initial, err := (&ProjectGeneration{Components: []*Result{child}}).Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	childEntry := projectEntryByName(t, initial.Manifest, "UserAcl")
	if childEntry.OutputType == "" || childEntry.OutputOrigin != "generated" {
		t.Fatalf("persisted child output = %+v", childEntry)
	}

	parent := compileProjectComponentAt(t, "Vendors", filepath.Join(sourceRoot, "vendor", "vendors.dql"), `
#setting($_ = $route('/v1/api/dev/vendors', 'GET'))
#define($_ = $Auth<?>(component/../user_acl))
SELECT 1 AS ID`)
	partial, err := (&ProjectGeneration{Components: []*Result{parent}}).Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(partial.Manifest.Components) != 2 {
		t.Fatalf("partial manifest = %+v", partial.Manifest)
	}
	parentEntry := projectEntryByName(t, partial.Manifest, "Vendors")
	input, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(parentEntry.Package), "input.go"))
	if err != nil || !strings.Contains(string(input), "kind=component") ||
		!strings.Contains(string(input), "in=GET:/v1/api/dev/user-acl") ||
		!strings.Contains(string(input), "UserAclOutput") {
		t.Fatalf("generated parent input = %q, %v", input, err)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("partial generated project does not compile: %v\n%s", runErr, output)
	}
}

func TestProjectGenerationRejectsReferencedManifestWithoutOutputMetadata(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	sourceRoot := t.TempDir()
	child := compileProjectComponentAt(t, "Child", filepath.Join(sourceRoot, "child.dql"), `
#setting($_ = $route('/child', 'GET'))
SELECT 1 AS ID`)
	initial, err := (&ProjectGeneration{Components: []*Result{child}}).Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	initial.Manifest.Components[0].OutputType = ""
	initial.Manifest.Components[0].OutputOrigin = ""
	store, err := newProjectMetadataStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.writeJSON(filepath.Join(root, projectMetadataDir, projectManifestFile), initial.Manifest); err != nil {
		t.Fatal(err)
	}
	parent := compileProjectComponentAt(t, "Parent", filepath.Join(sourceRoot, "parent.dql"), `
#setting($_ = $route('/parent', 'GET'))
#define($_ = $Child<?>(component/child))
SELECT 1 AS ID`)
	_, err = (&ProjectGeneration{Components: []*Result{parent}}).Generate(context.Background(), root)
	if err == nil || !strings.Contains(err.Error(), "has no output type metadata; regenerate that dependency") {
		t.Fatalf("Generate() error = %v", err)
	}
}

func TestProjectComponentReferenceRequiresMethodWhenSiblingHasMultipleRoutes(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	sourceRoot := t.TempDir()
	child := compileProjectComponentAt(t, "Child", filepath.Join(sourceRoot, "child.dql"), `
#setting($_ = $route('/child', 'GET'))
SELECT 1 AS ID`)
	child.Component.Routes = append(child.Component.Routes, &spec.Route{Method: "POST", Path: "/child"})
	parent := compileProjectComponentAt(t, "Parent", filepath.Join(sourceRoot, "parent.dql"), `
#setting($_ = $route('/parent', 'GET'))
#define($_ = $Child<?>(component/child))
SELECT 1 AS ID`)
	_, err := (&ProjectGeneration{Components: []*Result{parent, child}}).prepare(root)
	if err == nil || !strings.Contains(err.Error(), `component reference "child" is ambiguous`) {
		t.Fatalf("prepare() error = %v", err)
	}

	parent = compileProjectComponentAt(t, "Parent", filepath.Join(sourceRoot, "parent.dql"), `
#setting($_ = $route('/parent', 'GET'))
#define($_ = $Child<?>(component/POST:child))
SELECT 1 AS ID`)
	prepared, err := (&ProjectGeneration{Components: []*Result{parent, child}}).prepare(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range prepared {
		if item.compiled.Component.Name == "Parent" {
			if actual := spec.EffectiveParameters(item.compiled.Component.Parameters)[0].Source.Name; actual != "POST:/child" {
				t.Fatalf("component source = %q", actual)
			}
		}
	}
}

func TestProjectComponentReferenceRejectsMissingAndCycles(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	sourceRoot := t.TempDir()
	missing := compileProjectComponentAt(t, "Missing", filepath.Join(sourceRoot, "missing.dql"), `
#setting($_ = $route('/missing', 'GET'))
#define($_ = $Child<?>(component/not_there))
SELECT 1 AS ID`)
	if _, err := (&ProjectGeneration{Components: []*Result{missing}}).prepare(root); err == nil || !strings.Contains(err.Error(), `component reference "not_there" was not found`) {
		t.Fatalf("missing prepare() error = %v", err)
	}

	left := compileProjectComponentAt(t, "Left", filepath.Join(sourceRoot, "left.dql"), `
#setting($_ = $route('/left', 'GET'))
#define($_ = $Right<?>(component/right))
SELECT 1 AS ID`)
	right := compileProjectComponentAt(t, "Right", filepath.Join(sourceRoot, "right.dql"), `
#setting($_ = $route('/right', 'GET'))
#define($_ = $Left<?>(component/left))
SELECT 1 AS ID`)
	if _, err := (&ProjectGeneration{Components: []*Result{left, right}}).prepare(root); err == nil || !strings.Contains(err.Error(), "component dependency cycle") {
		t.Fatalf("cycle prepare() error = %v", err)
	}
}

func TestProjectComponentReferenceUsesLinkedOutputAuthority(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	const packagePath = "github.com/viant/datly/transcribe/testdata/linkedcontract"
	child, err := (&PackageCompilation{
		Source: &Source{
			Scope: packagePath, Name: "Users", Path: filepath.Join(t.TempDir(), "linked_child.dql"),
			Text: "#setting($_ = $route('/users', 'GET'))\nSELECT id FROM users",
		},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "ComponentInput", OutputType: "ComponentOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users", View: "Users"},
		},
		InputType: reflect.TypeOf(linkedcontract.ComponentInput{}), OutputType: reflect.TypeOf(linkedcontract.ComponentOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	parent := compileProjectComponentAt(t, "LinkedParent", filepath.Join(t.TempDir(), "linked_parent.dql"), `
#setting($_ = $route('/linked-parent', 'GET'))
#define($_ = $Child<?>(component/GET:/users))
SELECT 1 AS ID`)
	prepared, err := (&ProjectGeneration{Components: []*Result{parent, child}}).prepare(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range prepared {
		if item.compiled.Component.Name != "LinkedParent" {
			continue
		}
		param := spec.EffectiveParameters(item.compiled.Component.Parameters)[0]
		if param.TypeExpr != "*"+packagePath+".ComponentOutput" ||
			len(item.plan.Input.Fields) != 1 || !strings.HasSuffix(item.plan.Input.Fields[0].Type, ".ComponentOutput") ||
			!planImportsPackage(item.plan.Imports, packagePath) {
			t.Fatalf("linked component parameter = %+v, plan = %+v", param, item.plan.Input)
		}
		initial, generateErr := (&ProjectGeneration{Components: []*Result{child}}).Generate(context.Background(), root)
		if generateErr != nil {
			t.Fatal(generateErr)
		}
		childEntry := projectEntryByName(t, initial.Manifest, "Users")
		if childEntry.OutputType != packagePath+".ComponentOutput" || childEntry.OutputOrigin != "package" {
			t.Fatalf("persisted linked output = %+v", childEntry)
		}
		partial, generateErr := (&ProjectGeneration{Components: []*Result{parent}}).Generate(context.Background(), root)
		if generateErr != nil {
			t.Fatal(generateErr)
		}
		parentEntry := projectEntryByName(t, partial.Manifest, "LinkedParent")
		input, readErr := os.ReadFile(filepath.Join(root, filepath.FromSlash(parentEntry.Package), "input.go"))
		if readErr != nil || !strings.Contains(string(input), "linkedcontract.ComponentOutput") {
			t.Fatalf("persisted linked parent input = %q, %v", input, readErr)
		}
		command := exec.Command("go", "test", "-mod=mod", "./...")
		command.Dir = root
		if output, runErr := command.CombinedOutput(); runErr != nil {
			t.Fatalf("linked partial project does not compile: %v\n%s", runErr, output)
		}
		return
	}
	t.Fatal("linked parent was not prepared")
}

func compileProjectComponentAt(t *testing.T, name, path, text string) *Result {
	t.Helper()
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/project", Name: name, Path: path, Text: text,
	})
	if err != nil {
		t.Fatalf("compile %s: %v", name, err)
	}
	return result
}

func planImportsPackage(imports []spec.ImportSpec, packagePath string) bool {
	for _, item := range imports {
		if item.Package == packagePath {
			return true
		}
	}
	return false
}
