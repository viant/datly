package index

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
)

func writeFixture(t testing.TB, root, name, body string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func holderSource(component, method, path, extra string) string {
	return `package api
import xdatly "github.com/viant/xdatly"
type Input struct{}
type Output struct{}
type Holder struct {
  Route xdatly.Component[Input,Output] ` + "`" + `component:"` + component + `,path=` + path + `,method=` + method + `" mcp:"[{\"kind\":\"tool\"}]"` + "`" + `
}
` + extra
}

func TestBuilderIndexesMultiModuleSelectionDeterministically(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "app/go.mod", "module corp.example/app\ngo 1.25\n")
	writeFixture(t, root, "private/go.mod", "module corp.example/private\ngo 1.25\n")
	writeFixture(t, root, "app/api/holder.go", holderSource("Users", "GET", "/package-users", ""))
	writeFixture(t, root, "app/api/Users.dql", "#setting($_ = $route('/users/{id}', 'GET'))\n#settings($_ = $mcp('users.get'))\nSELECT 1")
	writeFixture(t, root, "private/api/holder.go", holderSource("Private", "POST", "/private", ""))

	config := Config{BaseDir: root, ModuleDirs: []string{"app", "private"}, Include: []string{"corp.example/app/api", "corp.example/private/api"}, Exclude: []string{"corp.example/private/api"}}
	first, err := (Builder{Config: config}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	second, err := (Builder{Config: config}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Fingerprint != second.Fingerprint || first.SelectionIdentity != second.SelectionIdentity {
		t.Fatalf("non-deterministic snapshots: %s/%s, %s/%s", first.Fingerprint, second.Fingerprint, first.SelectionIdentity, second.SelectionIdentity)
	}
	entries := first.Entries()
	if len(entries) != 1 || entries[0].Key().Scope != "corp.example/app/api" {
		t.Fatalf("include/exclude isolation failed: %+v", entries)
	}
	if _, _, _, ok := first.Route("GET", "/package-users"); ok {
		t.Fatal("package route remained exposed after DQL overlay")
	}
	entry, endpoint, params, ok := first.Route("GET", "/users/42")
	if !ok || entry.Key().Name != "Users" || endpoint.Path != "/users/{id}" || params["id"] != "42" {
		t.Fatalf("indexed route = %+v, %+v, %+v, %v", entry, endpoint, params, ok)
	}
	if _, ok := first.MCP(MCPIdentity{Kind: spec.MCPExposureTool, Name: "users.get"}); !ok {
		t.Fatal("MCP identity was not indexed")
	}
}

func TestBuilderRebuildInvalidatesAddRemoveTagAndSourceChanges(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "go.mod", "module example.com/app\ngo 1.25\n")
	path := "api/holder.go"
	writeFixture(t, root, path, holderSource("Users", "GET", "/users", "const revision = 1\n"))
	config := Config{BaseDir: root, Include: []string{"example.com/app/api"}}
	first, err := (Builder{Config: config}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, root, path, holderSource("Users", "PATCH", "/users/{id}", "const revision = 2\n"))
	second, err := (Builder{Config: config}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if first.Fingerprint == second.Fingerprint {
		t.Fatal("tag/source changes did not invalidate the snapshot")
	}
	if _, _, _, ok := second.Route("GET", "/users"); ok {
		t.Fatal("changed route remained exposed")
	}
	if _, _, _, ok := second.Route("PATCH", "/users/7"); !ok {
		t.Fatal("changed route was not indexed")
	}
	writeFixture(t, root, "api/orders.go", `package api
import xdatly "github.com/viant/xdatly"
type OrderInput struct{}
type OrderOutput struct{}
type OrderHolder struct { Route xdatly.Component[OrderInput,OrderOutput] `+"`"+`component:"Orders,path=/orders,method=GET"`+"`"+` }
`)
	third, err := (Builder{Config: config}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(third.Entries()) != 2 {
		t.Fatalf("added component missing: %+v", third.Entries())
	}
	if err := os.Remove(filepath.Join(root, filepath.FromSlash(path))); err != nil {
		t.Fatal(err)
	}
	fourth, err := (Builder{Config: config}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, ok := fourth.Route("PATCH", "/users/7"); ok || len(fourth.Entries()) != 1 {
		t.Fatalf("removed component remained reachable: %+v", fourth.Entries())
	}
}

func TestBuilderIndexesContractGeneratedAlternateRoutes(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/app"}).Write(t, root)
	writeFixture(t, root, "api/holder.go", `package api
import xdatly "github.com/viant/xdatly"
type Input struct { ID *int `+"`"+`parameter:"ID,kind=path,in=id,uri=/{id},required=true"`+"`"+` }
type Output struct{}
type Holder struct { Route xdatly.Component[Input,Output] `+"`"+`component:"Users,path=/users,method=GET"`+"`"+` }
`)
	snapshot, err := (Builder{Config: Config{BaseDir: root, Include: []string{"example.com/app/api"}}}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/users", "/users/42"} {
		if _, _, _, ok := snapshot.Route("GET", path); !ok {
			t.Fatalf("generated alternate route %q was not indexed", path)
		}
	}
}

func TestGenerationFailsClosedWhenIndexedSourceChangesBeforeLoad(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "go.mod", "module example.com/app\ngo 1.25\n")
	path := "api/holder.go"
	writeFixture(t, root, path, holderSource("Users", "GET", "/users", ""))
	snapshot, err := (Builder{Config: Config{BaseDir: root, Include: []string{"example.com/app/api"}}}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, root, path, holderSource("Users", "GET", "/changed", ""))
	called := false
	generation := newGeneration(1, snapshot, MaterializeFunc(func(context.Context, *Entry, Resolver) (*Loaded, error) {
		called = true
		return nil, nil
	}))
	entry := snapshot.Entries()[0]
	if _, err := generation.Load(context.Background(), entry.Key()); !errors.Is(err, ErrStaleSource) {
		t.Fatalf("load error = %v", err)
	}
	if called {
		t.Fatal("stale source reached materializer")
	}
	generation.retire()
}

func TestGenerationFingerprintsImportedPackageSources(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "app/go.mod", "module example.com/app\ngo 1.25\nreplace example.com/private => ../private\n")
	writeFixture(t, root, "private/go.mod", "module example.com/private\ngo 1.25\n")
	modelPath := "private/model/model.go"
	writeFixture(t, root, modelPath, "package model\ntype Input struct{ Value string }\n")
	writeFixture(t, root, "app/api/holder.go", `package api
import (
  xdatly "github.com/viant/xdatly"
  model "example.com/private/model"
)
type Output struct{}
type Holder struct { Route xdatly.Component[model.Input,Output] `+"`"+`component:"Imported,path=/imported,method=GET"`+"`"+` }
`)
	snapshot, err := (Builder{Config: Config{BaseDir: root, ModuleDirs: []string{"app", "private"}, Include: []string{"example.com/app/api"}}}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, root, modelPath, "package model\ntype Input struct{ Value int }\n")
	called := false
	generation := newGeneration(1, snapshot, MaterializeFunc(func(context.Context, *Entry, Resolver) (*Loaded, error) {
		called = true
		return nil, nil
	}))
	if _, err := generation.Load(context.Background(), snapshot.entries[0].Key()); !errors.Is(err, ErrStaleSource) {
		t.Fatalf("load error = %v", err)
	}
	if called {
		t.Fatal("changed imported package reached materializer")
	}
	generation.retire()
}

func TestGenerationFingerprintsDQLImportedPackageSources(t *testing.T) {
	root := t.TempDir()
	writeFixture(t, root, "app/go.mod", "module example.com/app\ngo 1.25\nreplace example.com/private => ../private\n")
	writeFixture(t, root, "private/go.mod", "module example.com/private\ngo 1.25\n")
	modelPath := "private/model/model.go"
	writeFixture(t, root, modelPath, "package model\ntype Input struct{ Value string }\n")
	writeFixture(t, root, "app/api/Imported.dql", `#import('model','example.com/private/model')
#setting($_ = $route('/imported', 'GET'))
#setting($_ = $input_type('model.Input'))
SELECT 1
`)
	snapshot, err := (Builder{Config: Config{BaseDir: root, ModuleDirs: []string{"app", "private"}, Include: []string{"example.com/app/api"}}}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	writeFixture(t, root, modelPath, "package model\ntype Input struct{ Value int }\n")
	called := false
	generation := newGeneration(1, snapshot, MaterializeFunc(func(context.Context, *Entry, Resolver) (*Loaded, error) {
		called = true
		return nil, nil
	}))
	if _, err := generation.Load(context.Background(), snapshot.entries[0].Key()); !errors.Is(err, ErrStaleSource) {
		t.Fatalf("load error = %v", err)
	}
	if called {
		t.Fatal("changed DQL import reached materializer")
	}
	generation.retire()
}
