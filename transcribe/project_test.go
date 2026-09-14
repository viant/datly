package transcribe

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	dtag "github.com/viant/datly/tag"
)

func TestProjectGenerationEmitsAuditableMultiComponentProject(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	users := compileProjectSource(t, "Users", "/users", "SELECT id FROM users")
	orders := compileProjectSource(t, "Orders", "/orders", "SELECT id FROM orders")

	generated, err := (&ProjectGeneration{Components: []*Result{orders, users}}).Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(generated.Components) != 2 || len(generated.Manifest.Components) != 2 ||
		generated.Manifest.Components[0].Key.Name != "Orders" || generated.Manifest.Components[1].Key.Name != "Users" {
		t.Fatalf("generated project = %+v", generated.Manifest)
	}
	for _, component := range generated.Manifest.Components {
		if component.Package == "" || component.IR == "" || component.Diagnostics == "" || len(component.Artifacts) < 4 || len(component.Routes) != 1 {
			t.Fatalf("project component = %+v", component)
		}
		for _, relative := range append([]string{filepath.Join(projectMetadataDir, component.IR), filepath.Join(projectMetadataDir, component.Diagnostics)}, component.Artifacts...) {
			if _, err = os.Stat(filepath.Join(root, filepath.FromSlash(relative))); err != nil {
				t.Fatalf("project artifact %q: %v", relative, err)
			}
		}
	}
	manifest := readPersistedProjectManifest(t, root)
	if len(manifest.Analysis.Components) != 2 || manifest.Analysis.Components[0].Handler != "reader" || manifest.Analysis.Components[1].Handler != "reader" {
		t.Fatalf("migration analysis = %+v", manifest.Analysis)
	}
	if _, err = os.Stat(filepath.Join(root, projectMetadataDir, "migration.json")); err != nil {
		t.Fatalf("migration report: %v", err)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	if output, runErr := command.CombinedOutput(); runErr != nil {
		t.Fatalf("generated project does not compile: %v\n%s", runErr, output)
	}
}

func TestProjectGenerationPartialRerunPreservesUnrelatedComponent(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	users := compileProjectSource(t, "Users", "/users", "SELECT id FROM users")
	orders := compileProjectSource(t, "Orders", "/orders", "SELECT id FROM orders")
	initial, err := (&ProjectGeneration{Components: []*Result{users, orders}}).Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	ordersEntry := projectEntryByName(t, initial.Manifest, "Orders")
	marker := filepath.Join(root, filepath.FromSlash(ordersEntry.Package), "owned_by_user.txt")
	if err = os.WriteFile(marker, []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}

	users = compileProjectSource(t, "Users", "/users", `#define($_ = $Search<string>(query/search).Optional())
SELECT id FROM users`)
	partial, err := (&ProjectGeneration{Components: []*Result{users}}).Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(partial.Manifest.Components) != 2 || projectEntryByName(t, partial.Manifest, "Orders").Package != ordersEntry.Package {
		t.Fatalf("partial manifest = %+v", partial.Manifest)
	}
	if content, err := os.ReadFile(marker); err != nil || string(content) != "keep" {
		t.Fatalf("unrelated component changed: %q, %v", content, err)
	}
	usersEntry := projectEntryByName(t, partial.Manifest, "Users")
	input, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(usersEntry.Package), "users_input.go"))
	if err != nil || !strings.Contains(string(input), "Search string") {
		t.Fatalf("updated component input = %q, %v", input, err)
	}
}

func TestProjectGenerationRejectsRouteCollisionBeforeEmission(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	users := compileProjectSource(t, "Users", "/shared", "SELECT id FROM users")
	orders := compileProjectSource(t, "Orders", "/shared", "SELECT id FROM orders")
	_, err := (&ProjectGeneration{Components: []*Result{users, orders}}).Generate(context.Background(), root)
	if err == nil || !strings.Contains(err.Error(), `route "GET /shared" is shared`) {
		t.Fatalf("Generate() error = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "generated")); !os.IsNotExist(statErr) {
		t.Fatalf("collision emitted generated packages: %v", statErr)
	}
}

func TestProjectGenerationRejectsExistingRouteCollisionOnPartialRun(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	users := compileProjectSource(t, "Users", "/shared", "SELECT id FROM users")
	if _, err := (&ProjectGeneration{Components: []*Result{users}}).Generate(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	orders := compileProjectSource(t, "Orders", "/shared", "SELECT id FROM orders")
	if _, err := (&ProjectGeneration{Components: []*Result{orders}}).Generate(context.Background(), root); err == nil || !strings.Contains(err.Error(), `route "GET /shared" is shared`) {
		t.Fatalf("Generate() error = %v", err)
	}
	if manifest := readPersistedProjectManifest(t, root); len(manifest.Components) != 1 || manifest.Components[0].Key.Name != "Users" {
		t.Fatalf("manifest changed after collision: %+v", manifest)
	}
}

func TestProjectGenerationSerializesConcurrentPartialRuns(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	components := []*Result{
		compileProjectSource(t, "Users", "/users", "SELECT id FROM users"),
		compileProjectSource(t, "Orders", "/orders", "SELECT id FROM orders"),
	}
	errors := make(chan error, len(components))
	var wait sync.WaitGroup
	for _, component := range components {
		wait.Add(1)
		go func(component *Result) {
			defer wait.Done()
			_, err := (&ProjectGeneration{Components: []*Result{component}}).Generate(context.Background(), root)
			errors <- err
		}(component)
	}
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	if manifest := readPersistedProjectManifest(t, root); len(manifest.Components) != 2 {
		t.Fatalf("concurrent manifest = %+v", manifest)
	}
}

func TestProjectGenerationRejectsMetadataSymlinkBeforeEmission(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	metadata := filepath.Join(root, projectMetadataDir)
	if err := os.Mkdir(metadata, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(metadata, "ir")); err != nil {
		t.Fatal(err)
	}
	users := compileProjectSource(t, "Users", "/users", "SELECT id FROM users")
	_, err := (&ProjectGeneration{Components: []*Result{users}}).Generate(context.Background(), root)
	if err == nil || !strings.Contains(err.Error(), "unsupported symlink") {
		t.Fatalf("Generate() error = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "generated")); !os.IsNotExist(statErr) {
		t.Fatalf("symlink failure emitted generated package: %v", statErr)
	}
}

func TestProjectGenerationRejectsMetadataRootSymlinkBeforeRead(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, projectManifestFile), []byte(`{"version":1}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, projectMetadataDir)); err != nil {
		t.Fatal(err)
	}
	users := compileProjectSource(t, "Users", "/users", "SELECT id FROM users")
	_, err := (&ProjectGeneration{Components: []*Result{users}}).Generate(context.Background(), root)
	if err == nil || !strings.Contains(err.Error(), "unsupported symlink") {
		t.Fatalf("Generate() error = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(root, "generated")); !os.IsNotExist(statErr) {
		t.Fatalf("symlink failure emitted generated package: %v", statErr)
	}
}

func TestProjectGenerationIncludesPackageLinkedDependenciesAndHandlerAnalysis(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	const packagePath = "github.com/viant/datly/transcribe"
	compiled, err := (&PackageCompilation{
		Source: &Source{Scope: packagePath, Name: "Users", Path: "users.sql", Text: "#setting($_ = $route('/users', 'GET'))\nSELECT id FROM users"},
		Component: &bootstrap.RouteSource{
			FieldName: "Users", PackagePath: packagePath,
			InputType: "PackageCompileInput", OutputType: "PackageCompileOutput",
			Tag: dtag.Component{Method: "GET", Path: "/users", Handler: "HandleUsers"},
		},
		InputType: reflect.TypeOf(PackageCompileInput{}), OutputType: reflect.TypeOf(PackageCompileOutput{}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	project, err := (&ProjectGeneration{Components: []*Result{compiled}}).Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	entry := project.Manifest.Components[0]
	if !containsProjectValue(entry.Dependencies, packagePath) || len(project.Manifest.Analysis.Components) != 1 ||
		project.Manifest.Analysis.Components[0].Handler != "custom" || project.Manifest.Analysis.Components[0].InputOwnership != "linked" {
		t.Fatalf("package project metadata = component:%+v analysis:%+v", entry, project.Manifest.Analysis)
	}
}

func TestProjectGenerationPreflightsAllPackageOwnersBeforeEmission(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	users := compileProjectSource(t, "Users", "/users", "SELECT id FROM users")
	orders := compileProjectSource(t, "Orders", "/orders", "SELECT id FROM orders")
	initial, err := (&ProjectGeneration{Components: []*Result{users, orders}}).Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	usersEntry := projectEntryByName(t, initial.Manifest, "Users")
	ordersEntry := projectEntryByName(t, initial.Manifest, "Orders")
	usersInput := filepath.Join(root, filepath.FromSlash(usersEntry.Package), "users_input.go")
	before, err := os.ReadFile(usersInput)
	if err != nil {
		t.Fatal(err)
	}
	ownerManifest := filepath.Join(root, filepath.FromSlash(ordersEntry.Package), ".datly-gen.json")
	if err = os.WriteFile(ownerManifest, []byte("{\n  \"version\": 2,\n  \"owner\": \"Other\",\n  \"files\": []\n}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	users = compileProjectSource(t, "Users", "/users", `#define($_ = $Search<string>(query/search).Optional())
SELECT id FROM users`)
	_, err = (&ProjectGeneration{Components: []*Result{users, orders}}).Generate(context.Background(), root)
	if err == nil || !strings.Contains(err.Error(), "generated package is owned by component") {
		t.Fatalf("Generate() error = %v", err)
	}
	after, err := os.ReadFile(usersInput)
	if err != nil || string(after) != string(before) {
		t.Fatalf("earlier component changed before owner failure: before=%q after=%q err=%v", before, after, err)
	}
}

func TestProjectGenerationPreflightsUnownedPackageFilesBeforeEmission(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	users := compileProjectSource(t, "Users", "/users", "SELECT id FROM users")
	orders := compileProjectSource(t, "Orders", "/orders", "SELECT id FROM orders")
	initial, err := (&ProjectGeneration{Components: []*Result{users, orders}}).Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	usersEntry := projectEntryByName(t, initial.Manifest, "Users")
	ordersEntry := projectEntryByName(t, initial.Manifest, "Orders")
	usersInput := filepath.Join(root, filepath.FromSlash(usersEntry.Package), "users_input.go")
	before, err := os.ReadFile(usersInput)
	if err != nil {
		t.Fatal(err)
	}
	ordersPackage := filepath.Join(root, filepath.FromSlash(ordersEntry.Package))
	if err = os.Remove(filepath.Join(ordersPackage, ".datly-gen.json")); err != nil {
		t.Fatal(err)
	}
	users = compileProjectSource(t, "Users", "/users", `#define($_ = $Search<string>(query/search).Optional())
SELECT id FROM users`)
	_, err = (&ProjectGeneration{Components: []*Result{users, orders}}).Generate(context.Background(), root)
	if err == nil || !strings.Contains(err.Error(), "collides with an unowned package file") {
		t.Fatalf("Generate() error = %v", err)
	}
	after, err := os.ReadFile(usersInput)
	if err != nil || string(after) != string(before) {
		t.Fatalf("earlier component changed before file collision: before=%q after=%q err=%v", before, after, err)
	}
}

func TestProjectGenerationPreflightsUntrackedFileInOwnedPackage(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	users := compileProjectSource(t, "Users", "/users", "SELECT id FROM users")
	orders := compileProjectSource(t, "Orders", "/orders", "SELECT id FROM orders")
	initial, err := (&ProjectGeneration{Components: []*Result{users, orders}}).Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	usersEntry := projectEntryByName(t, initial.Manifest, "Users")
	ordersEntry := projectEntryByName(t, initial.Manifest, "Orders")
	usersInput := filepath.Join(root, filepath.FromSlash(usersEntry.Package), "users_input.go")
	before, err := os.ReadFile(usersInput)
	if err != nil {
		t.Fatal(err)
	}
	ordersPackage := filepath.Join(root, filepath.FromSlash(ordersEntry.Package))
	untracked := filepath.Join(ordersPackage, "orders_input.go")
	manifestPath := filepath.Join(ordersPackage, ".datly-gen.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Version int      `json:"version"`
		Owner   string   `json:"owner"`
		Files   []string `json:"files"`
	}
	if err = json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatal(err)
	}
	retained := manifest.Files[:0]
	for _, path := range manifest.Files {
		if path != "orders_input.go" {
			retained = append(retained, path)
		}
	}
	manifest.Files = retained
	manifestData, err = json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(manifestPath, append(manifestData, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(untracked, []byte("package userowned\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	users = compileProjectSource(t, "Users", "/users", `#define($_ = $Search<string>(query/search).Optional())
SELECT id FROM users`)
	_, err = (&ProjectGeneration{Components: []*Result{users, orders}}).Generate(context.Background(), root)
	if err == nil || !strings.Contains(err.Error(), "collides with an unowned package file") {
		t.Fatalf("Generate() error = %v", err)
	}
	after, err := os.ReadFile(usersInput)
	if err != nil || string(after) != string(before) {
		t.Fatalf("earlier component changed before owned-package collision: before=%q after=%q err=%v", before, after, err)
	}
	content, err := os.ReadFile(untracked)
	if err != nil || string(content) != "package userowned\n" {
		t.Fatalf("untracked package file changed: %q, %v", content, err)
	}
}

func compileProjectSource(t *testing.T, name, route, sqlText string) *Result {
	t.Helper()
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/project", Name: name, Path: strings.ToLower(name) + ".sql",
		Text: "#setting($_ = $route('" + route + "', 'GET'))\n" + sqlText,
	})
	if err != nil {
		t.Fatalf("compile %s: %v", name, err)
	}
	return result
}

func readPersistedProjectManifest(t *testing.T, root string) *ProjectManifest {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, projectMetadataDir, projectManifestFile))
	if err != nil {
		t.Fatal(err)
	}
	manifest := &ProjectManifest{}
	if err = json.Unmarshal(data, manifest); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func projectEntryByName(t *testing.T, manifest *ProjectManifest, name string) ProjectComponent {
	t.Helper()
	for _, component := range manifest.Components {
		if component.Key.Name == name {
			return component
		}
	}
	t.Fatalf("component %q not found in %+v", name, manifest)
	return ProjectComponent{}
}

func containsProjectValue(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}
