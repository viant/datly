package typectx

import (
	"context"
	"path"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/x"
	xast "github.com/viant/x/loader/ast"
)

func TestResolver_MemFS_DefaultPackageResolution(t *testing.T) {
	resolver := memFSResolver(t, baseTypeMapFS(), []string{"root/perf"}, &Context{
		DefaultPackage: "example.com/acme/perf",
	})

	key, err := resolver.Resolve("Order")
	require.NoError(t, err)
	require.Equal(t, "example.com/acme/perf.Order", key)
}

func TestResolver_MemFS_AliasImportResolution(t *testing.T) {
	resolver := memFSResolver(t, baseTypeMapFS(), []string{"root/perf"}, &Context{
		Imports: []Import{
			{Alias: "pf", Package: "example.com/acme/perf"},
		},
	})

	key, err := resolver.Resolve("pf.Order")
	require.NoError(t, err)
	require.Equal(t, "example.com/acme/perf.Order", key)
}

func TestResolver_MemFS_AmbiguityDetection(t *testing.T) {
	resolver := memFSResolver(t, baseTypeMapFS(), []string{"root/perf", "root/shared"}, &Context{
		Imports: []Import{
			{Alias: "pf", Package: "example.com/acme/perf"},
			{Alias: "sh", Package: "example.com/acme/shared"},
		},
	})

	key, err := resolver.Resolve("Fee")
	require.Empty(t, key)
	require.Error(t, err)
	amb, ok := err.(*AmbiguityError)
	require.True(t, ok)
	require.Equal(t, []string{
		"example.com/acme/perf.Fee",
		"example.com/acme/shared.Fee",
	}, amb.Candidates)
}

func TestResolver_MemFS_ProvenanceCapture(t *testing.T) {
	resolver := memFSResolver(t, baseTypeMapFS(), []string{"root/perf"}, &Context{
		DefaultPackage: "example.com/acme/perf",
	})

	resolved, err := resolver.ResolveWithProvenance("Order")
	require.NoError(t, err)
	require.NotNil(t, resolved)
	require.Equal(t, "example.com/acme/perf.Order", resolved.ResolvedKey)
	require.Equal(t, "default_package", resolved.MatchKind)
	require.Equal(t, "ast_type", resolved.Provenance.Kind)
	require.Equal(t, "example.com/acme/perf", resolved.Provenance.Package)
	require.Equal(t, "root/perf/types.go", resolved.Provenance.File)
}

func memFSResolver(t *testing.T, fsys fstest.MapFS, packageDirs []string, ctx *Context) *Resolver {
	t.Helper()
	registry := x.NewRegistry()
	provenance := map[string]Provenance{}
	for _, dir := range packageDirs {
		pkg, err := xast.LoadPackageFS(context.Background(), fsys, dir)
		require.NoError(t, err)

		fileByType := map[string]string{}
		for _, file := range pkg.Files {
			if file == nil {
				continue
			}
			for _, item := range file.Types {
				if item == nil || item.Name == "" {
					continue
				}
				fileByType[item.Name] = path.Join(dir, file.Name)
			}
		}
		for _, item := range pkg.Types {
			if item == nil || item.Name == "" {
				continue
			}
			aType := &x.Type{
				Name:    item.Name,
				PkgPath: pkg.PkgPath,
			}
			registry.Register(aType)
			provenance[aType.Key()] = Provenance{
				Package: pkg.PkgPath,
				File:    fileByType[item.Name],
				Kind:    "ast_type",
			}
		}
	}
	return NewResolverWithProvenance(registry, ctx, provenance)
}

func baseTypeMapFS() fstest.MapFS {
	return fstest.MapFS{
		"root/go.mod":           &fstest.MapFile{Data: []byte("module example.com/acme\n\ngo 1.23\n")},
		"root/perf/types.go":    &fstest.MapFile{Data: []byte("package perf\n\ntype Order struct{}\ntype Fee struct{}\n")},
		"root/shared/types.go":  &fstest.MapFile{Data: []byte("package shared\n\ntype Fee struct{}\n")},
		"root/ignore/other.txt": &fstest.MapFile{Data: []byte("skip")},
	}
}
