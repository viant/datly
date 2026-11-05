package dependency

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	xmodule "github.com/viant/x/module"
)

func TestDeclarationImports(t *testing.T) {
	type useCase struct {
		description, source string
		expected            []string
	}
	for _, tc := range []useCase{
		{"renamed field", `import renamed "example.com/dep"; type Input struct { Value renamed.Item }`, []string{"example.com/dep"}},
		{"alias", `import "example.com/dep"; type Input = dep.Item`, []string{"example.com/dep"}},
		{"constraint", `import "example.com/dep"; type Input[T dep.Constraint] struct { Value T }`, []string{"example.com/dep"}},
		{"embedding", `import "example.com/dep"; type Input struct { dep.Item }`, []string{"example.com/dep"}},
		{"constant chain", `import "example.com/dep"; const Count = dep.Size; const Size = Count; type Input [Size]int`, []string{"example.com/dep"}},
		{"inherited constant", "import \"example.com/dep\"; const (A = dep.Size; B); type Input [B]int", []string{"example.com/dep"}},
		{"field name is not constant use", `import "example.com/dep"; const Size = dep.Size; type Input struct { Size int }`, []string{}},
		{"selector member is not local constant", `import ("example.com/dep"; "example.com/unused"); const Size = unused.Size; type Input [dep.Size]int`, []string{"example.com/dep"}},
		{"signature only", `import "example.com/dep"; type Input struct{}; func (*Input) Transform(v dep.Item) dep.Item { return v }`, []string{"example.com/dep"}},
		{"body only", `import "example.com/dep"; type Input struct{}; func (*Input) Run() { dep.Run() }`, []string{}},
		{"dot import", `import . "example.com/dep"; type Input struct { Value Item }`, []string{"example.com/dep"}},
	} {
		t.Run(tc.description, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/root\n\ngo 1.25\n"), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(root, "input.go"), []byte("package root\n"+tc.source), 0600))
			workspace, err := (xmodule.LocalWorkspace{BaseDir: root}).Resolve(t.Context())
			require.NoError(t, err)
			pkg, err := LoadPackage(t.Context(), workspace, "example.com/root")
			require.NoError(t, err)
			actual := []string{}
			for _, imp := range declarationImports(pkg) {
				actual = append(actual, imp.Path)
			}
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestDeclarationImportsUsesMethodFileScope(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod":    "module example.com/root\n\ngo 1.25\n",
		"input.go":  "package root\nimport alias \"example.com/field\"\ntype Input struct { Value alias.Item }\n",
		"method.go": "package root\nimport alias \"example.com/method\"\nfunc (*Input) Transform(v alias.Item) alias.Item { return v }\n",
	}
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(content), 0600))
	}
	workspace, err := (xmodule.LocalWorkspace{BaseDir: root}).Resolve(t.Context())
	require.NoError(t, err)
	pkg, err := LoadPackage(t.Context(), workspace, "example.com/root")
	require.NoError(t, err)
	actual := []string{}
	for _, imp := range declarationImports(pkg) {
		actual = append(actual, imp.Path)
	}
	require.Equal(t, []string{"example.com/field", "example.com/method"}, actual)
}

func TestDeclarationImportsInheritedConstantAcrossFiles(t *testing.T) {
	root := t.TempDir()
	for name, content := range map[string]string{
		"go.mod":    "module example.com/root\n\ngo 1.25\n",
		"first.go":  "package root\nconst (A = Count; B)\ntype Input [B]int\n",
		"second.go": "package root\nimport \"example.com/dep\"\nconst Count = dep.Size\n",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(content), 0600))
	}
	workspace, err := (xmodule.LocalWorkspace{BaseDir: root}).Resolve(t.Context())
	require.NoError(t, err)
	pkg, err := LoadPackage(t.Context(), workspace, "example.com/root")
	require.NoError(t, err)
	imports := declarationImports(pkg)
	require.Len(t, imports, 1)
	require.Equal(t, "example.com/dep", imports[0].Path)
}
