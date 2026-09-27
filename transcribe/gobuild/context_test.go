package gobuild

import (
	"context"
	"go/types"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWorkspaceAndRelativeReplacements(t *testing.T) {
	root := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		file := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(file), 0700))
		require.NoError(t, os.WriteFile(file, []byte(content), 0600))
	}
	write("go.work", "go 1.25.0\nuse ./app\n")
	write("dependency/go.mod", "module example.com/dependency\ngo 1.25.0\n")
	write("dependency/value.go", "package dependency\nconst Value = 42\n")
	write("app/go.mod", "module example.com/app\ngo 1.25.0\nrequire example.com/dependency v0.0.0\nreplace example.com/dependency => ../dependency\n")
	write("app/business/value.go", "package business\nimport \"example.com/dependency\"\nconst Value = dependency.Value\nfunc init() { panic(\"compile must not execute init\") }\n")
	build := (&Context{Dir: filepath.Join(root, "app"), Env: []string{"GOWORK=" + filepath.Join(root, "go.work")}}).WithContext(context.Background())
	loader, err := build.Importer("example.com/app/business")
	require.NoError(t, err)
	pkg, err := loader.Import("example.com/app/business")
	require.NoError(t, err)
	require.NotNil(t, pkg.Scope().Lookup("Value"))
	target := filepath.Join(root, "app", "new", "endpoint.go")
	err = build.Validate("example.com/app/new", map[string][]byte{target: []byte("package endpoint\nimport \"example.com/app/business\"\nvar Value = business.Value\n")})
	require.NoError(t, err)
	_, err = os.Stat(filepath.Dir(target))
	require.True(t, os.IsNotExist(err))
	// A planned removal cannot be masked by the old on-disk declaration.
	write("app/new/old.go", "package endpoint\nconst Old = 1\n")
	err = build.Validate("example.com/app/new", map[string][]byte{target: []byte("package endpoint\nvar Value = Old\n"), filepath.Join(root, "app/new/old.go"): nil})
	require.ErrorContains(t, err, "undefined: Old")
	data, err := os.ReadFile(filepath.Join(root, "app/new/old.go"))
	require.NoError(t, err)
	require.Contains(t, string(data), "const Old = 1")
	// Same import path in another checkout must not make the original package
	// stand in for files that are actually going to be published elsewhere.
	err = build.Validate("example.com/app/business", map[string][]byte{filepath.Join(root, "other/business/endpoint.go"): []byte("package business\nvar Broken = Missing\n")})
	require.ErrorContains(t, err, "outside selected build package")
}

func TestBuildPreservesVendoring(t *testing.T) {
	root := t.TempDir()
	write := func(name, content string) {
		t.Helper()
		file := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(file), 0700))
		require.NoError(t, os.WriteFile(file, []byte(content), 0600))
	}
	write("dep/go.mod", "module example.com/dep\ngo 1.25.0\n")
	write("dep/value.go", "package dep\ntype Value string\n")
	write("app/go.mod", "module example.com/app\ngo 1.25.0\nrequire example.com/dep v0.0.0\nreplace example.com/dep => ../dep\n")
	write("app/vendor/modules.txt", "# example.com/dep v0.0.0 => ../dep\n## explicit; go 1.25.0\nexample.com/dep\n# example.com/dep => ../dep\n")
	write("app/vendor/example.com/dep/value.go", "package dep\ntype Value int\n")
	write("app/business/value.go", "package business\nimport \"example.com/dep\"\nvar Value dep.Value\n")
	for _, flags := range []string{"", "-mod=vendor", "-mod=mod -mod=vendor"} {
		build := &Context{Dir: filepath.Join(root, "app"), Env: []string{"GOWORK=off", "GOFLAGS=" + flags}}
		loader, err := build.Importer("example.com/app/business")
		require.NoError(t, err)
		pkg, err := loader.Import("example.com/app/business")
		require.NoError(t, err)
		require.Equal(t, types.Int, pkg.Scope().Lookup("Value").Type().Underlying().(*types.Basic).Kind())
		err = build.Validate("example.com/app/endpoint", map[string][]byte{filepath.Join(root, "app/endpoint/endpoint.go"): []byte("package endpoint\nimport \"example.com/app/business\"\nvar Value = business.Value + 1\n")})
		require.NoError(t, err)
	}
}
