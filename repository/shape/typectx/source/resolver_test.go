package source

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolver_ResolvePackageDir_UsesLocalReplace(t *testing.T) {
	root := t.TempDir()
	projectDir := filepath.Join(root, "project")
	modelsDir := filepath.Join(root, "shared-models")
	require.NoError(t, os.MkdirAll(filepath.Join(projectDir, "internal"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(modelsDir, "mdp"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(modelsDir, "go.mod"), []byte("module github.com/acme/models\n\ngo 1.25\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/project
go 1.25
replace github.com/acme/models => ../shared-models
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(modelsDir, "mdp", "types.go"), []byte("package mdp\ntype Order struct{}\n"), 0o644))

	resolver, err := New(Config{
		ProjectDir:         projectDir,
		UseGoModuleResolve: true,
		UseGOPATHFallback:  false,
	})
	require.NoError(t, err)
	dir, err := resolver.ResolvePackageDir("github.com/acme/models/mdp")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(modelsDir, "mdp"), dir)
}

func TestResolver_ResolveTypeFile_RespectsTrustedRoots(t *testing.T) {
	root := t.TempDir()
	projectDir := filepath.Join(root, "project")
	modelsDir := filepath.Join(root, "shared-models")
	require.NoError(t, os.MkdirAll(filepath.Join(projectDir, "internal"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(modelsDir, "mdp"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte(`module example.com/project
go 1.25
replace github.com/acme/models => ../shared-models
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(modelsDir, "mdp", "types.go"), []byte("package mdp\ntype Order struct{}\n"), 0o644))

	denyResolver, err := New(Config{
		ProjectDir:         projectDir,
		UseGoModuleResolve: true,
		UseGOPATHFallback:  false,
	})
	require.NoError(t, err)
	_, err = denyResolver.ResolveTypeFile("github.com/acme/models/mdp", "Order")
	require.Error(t, err)

	allowResolver, err := New(Config{
		ProjectDir:         projectDir,
		AllowedSourceRoots: []string{modelsDir},
		UseGoModuleResolve: true,
		UseGOPATHFallback:  false,
	})
	require.NoError(t, err)
	file, err := allowResolver.ResolveTypeFile("github.com/acme/models/mdp", "Order")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(modelsDir, "mdp", "types.go"), file)
}

func TestResolver_ResolvePackageDir_GOPATHFallback(t *testing.T) {
	root := t.TempDir()
	projectDir := filepath.Join(root, "project")
	gopath := filepath.Join(root, "gopath")
	require.NoError(t, os.MkdirAll(filepath.Join(projectDir, "internal"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/project\ngo 1.25\n"), 0o644))
	legacyDir := filepath.Join(gopath, "src", "github.com", "legacy", "models")
	require.NoError(t, os.MkdirAll(legacyDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(legacyDir, "types.go"), []byte("package models\ntype Legacy struct{}\n"), 0o644))

	orig := os.Getenv("GOPATH")
	require.NoError(t, os.Setenv("GOPATH", gopath))
	defer func() { _ = os.Setenv("GOPATH", orig) }()

	resolver, err := New(Config{
		ProjectDir:         projectDir,
		UseGoModuleResolve: false,
		UseGOPATHFallback:  true,
	})
	require.NoError(t, err)
	dir, err := resolver.ResolvePackageDir("github.com/legacy/models")
	require.NoError(t, err)
	require.Equal(t, legacyDir, dir)
}
