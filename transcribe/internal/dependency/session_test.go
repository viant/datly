package dependency

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/typecatalog"
	xmodule "github.com/viant/x/module"
)

func TestSessionRefreshesOnlyChangedPackages(t *testing.T) {
	for _, change := range []string{"edit", "add", "remove", "invalid"} {
		t.Run(change, func(t *testing.T) {
			root := t.TempDir()
			write := func(name, text string) {
				require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0755))
				require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(text), 0600))
			}
			write("go.mod", "module example.com/cache\n\ngo 1.25\n")
			write("one/type.go", "package one\ntype First struct { ID int }")
			write("two/type.go", "package two\ntype Other struct { ID int }")
			workspace, err := (xmodule.LocalWorkspace{BaseDir: root}).Resolve(t.Context())
			require.NoError(t, err)
			session := &Session{}
			ctx := WithSession(t.Context(), session)
			first, err := LoadPackage(ctx, workspace, "example.com/cache/one")
			require.NoError(t, err)
			_, err = LoadPackage(ctx, workspace, "example.com/cache/two")
			require.NoError(t, err)
			first.Types[0].Name = "Mutated"
			again, err := LoadPackage(ctx, workspace, "example.com/cache/one")
			require.NoError(t, err)
			require.Equal(t, "First", again.Types[0].Name)
			switch change {
			case "edit":
				write("one/type.go", "package one\ntype Fresh struct { ID int }")
			case "add":
				write("one/new.go", "package one\ntype Fresh struct { Name string }")
			case "remove":
				require.NoError(t, os.Remove(filepath.Join(root, "one/type.go")))
			case "invalid":
				write("one/type.go", "package one\ntype")
			}
			changed, err := LoadPackage(ctx, workspace, "example.com/cache/one")
			if change == "remove" || change == "invalid" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.True(t, changed.HasType("Fresh"))
			}
			_, err = LoadPackage(ctx, workspace, "example.com/cache/two")
			require.NoError(t, err)
			require.Equal(t, 3, session.Parses)
			require.Equal(t, 2, session.Hits)
		})
	}
}

func TestSessionCatalogOwnsMutableSyntax(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/cache\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "type.go"), []byte("package cache\ntype Record struct { ID int }"), 0600))
	workspace, err := (xmodule.LocalWorkspace{BaseDir: root}).Resolve(t.Context())
	require.NoError(t, err)
	session := &Session{}
	ctx := WithSession(t.Context(), session)
	pkg, err := LoadPackage(ctx, workspace, "example.com/cache")
	require.NoError(t, err)
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg))
	typ, _, err := catalog.Resolve(typecatalog.PackageAuthority, "example.com/cache.Record")
	require.NoError(t, err)
	typ.SynteticType.TypeSpec.Name.Name = "Changed"
	again, err := LoadPackage(ctx, workspace, "example.com/cache")
	require.NoError(t, err)
	require.Equal(t, "Record", again.Types[0].TypeSpec.Name.Name)
	require.Same(t, again.Types[0], again.Files[0].Types[0])
	require.Equal(t, 1, session.Parses)
	require.Equal(t, 1, session.Hits)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = LoadPackage(canceled, workspace, "example.com/cache")
	require.ErrorIs(t, err, context.Canceled)
}
