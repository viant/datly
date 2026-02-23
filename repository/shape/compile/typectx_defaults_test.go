package compile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/typectx"
)

func TestApplyTypeContextDefaults_Matrix(t *testing.T) {
	layout := defaultCompilePathLayout()

	projectDir := t.TempDir()
	err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module github.vianttech.com/viant/platform\n\ngo 1.23\n"), 0o644)
	require.NoError(t, err)
	source := &shape.Source{
		Path: filepath.Join(projectDir, "dql", "platform", "taxonomy", "taxonomy.dql"),
	}

	t.Run("inferred only", func(t *testing.T) {
		got := applyTypeContextDefaults(nil, source, nil, layout)
		require.NotNil(t, got)
		require.Equal(t, "pkg/platform/taxonomy", got.PackageDir)
		require.Equal(t, "taxonomy", got.PackageName)
		require.Equal(t, "github.vianttech.com/viant/platform/pkg/platform/taxonomy", got.PackagePath)
	})

	t.Run("directive context wins over inferred", func(t *testing.T) {
		input := &typectx.Context{
			DefaultPackage: "github.com/acme/manual",
			PackageDir:     "pkg/manual",
			PackageName:    "manual",
			PackagePath:    "github.com/acme/manual",
		}
		got := applyTypeContextDefaults(input, source, nil, layout)
		require.NotNil(t, got)
		require.Equal(t, "pkg/manual", got.PackageDir)
		require.Equal(t, "manual", got.PackageName)
		require.Equal(t, "github.com/acme/manual", got.PackagePath)
		require.Equal(t, "github.com/acme/manual", got.DefaultPackage)
	})

	t.Run("compile override wins over both", func(t *testing.T) {
		input := &typectx.Context{
			PackageDir:  "pkg/manual",
			PackageName: "manual",
			PackagePath: "github.com/acme/manual",
		}
		got := applyTypeContextDefaults(input, source, &shape.CompileOptions{
			TypePackageDir:  "pkg/override",
			TypePackageName: "override",
			TypePackagePath: "github.com/acme/override",
		}, layout)
		require.NotNil(t, got)
		require.Equal(t, "pkg/override", got.PackageDir)
		require.Equal(t, "override", got.PackageName)
		require.Equal(t, "github.com/acme/override", got.PackagePath)
	})

	t.Run("explicitly disable inference", func(t *testing.T) {
		disabled := false
		got := applyTypeContextDefaults(nil, source, &shape.CompileOptions{
			InferTypeContext: &disabled,
		}, layout)
		require.Nil(t, got)
	})
}
