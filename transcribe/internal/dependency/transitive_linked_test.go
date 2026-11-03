package dependency

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx/types"
	xmodule "github.com/viant/x/module"
)

func TestLoadImportsIncludesTransitiveLinkedTypes(t *testing.T) {
	type useCase struct {
		description string
		direct      bool
	}
	for _, tc := range []useCase{{"direct import", true}, {"nested local import", false}} {
		t.Run(tc.description, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/transitive\n\ngo 1.25\n"), 0600))
			require.NoError(t, os.WriteFile(filepath.Join(root, "event.go"), []byte("package transitive\nimport \"github.com/viant/sqlx/types\"\ntype Event struct { Processed types.BitBool }\n"), 0600))
			workspace, err := (xmodule.LocalWorkspace{BaseDir: root}).Resolve(t.Context())
			require.NoError(t, err)
			catalog := typecatalog.NewCatalog()
			imports := map[string]bool{"example.com/transitive": true}
			if tc.direct {
				imports["github.com/viant/sqlx/types"] = true
			}
			_, err = (&Loader{Workspace: workspace, Catalog: catalog}).LoadImports(t.Context(), imports)
			require.NoError(t, err)
			descriptor, _, err := catalog.Resolve(typecatalog.PackageAuthority, "github.com/viant/sqlx/types.BitBool")
			require.NoError(t, err)
			require.NotNil(t, descriptor)
			require.Equal(t, reflect.TypeFor[types.BitBool](), descriptor.Type)
		})
	}
}

func TestLoadImportsExcludesImplementationOnlyDependencies(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/scoped\n\ngo 1.25\n"), 0600))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "implementation"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(root, "implementation", "service.go"), []byte("package implementation\nfunc Run() {}\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "event.go"), []byte("package scoped\nimport (\"example.com/scoped/implementation\"; \"github.com/viant/sqlx/types\")\ntype Event struct { Processed types.BitBool }\nfunc Run() { implementation.Run() }\n"), 0600))
	workspace, err := (xmodule.LocalWorkspace{BaseDir: root}).Resolve(t.Context())
	require.NoError(t, err)
	catalog := typecatalog.NewCatalog()
	loaded, err := (&Loader{Workspace: workspace, Catalog: catalog}).LoadImports(t.Context(), map[string]bool{"example.com/scoped": true})
	require.NoError(t, err)
	require.NotContains(t, loaded, "example.com/scoped/implementation")
	descriptor, _, err := catalog.Resolve(typecatalog.PackageAuthority, "github.com/viant/sqlx/types.BitBool")
	require.NoError(t, err)
	require.NotNil(t, descriptor)
	require.Equal(t, reflect.TypeFor[types.BitBool](), descriptor.Type)
}
