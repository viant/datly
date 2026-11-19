package generate

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestNativeWriterReplacesStructQLWithGoCodec(t *testing.T) {
	for _, ownership := range []string{"retired", "field URI", "linked row URI", "other filesystem"} {
		t.Run(ownership, func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			dir := filepath.Join(root, "orders")
			require.NoError(t, os.Mkdir(dir, 0755))
			id := reflect.StructField{Name: "Id", Type: reflect.TypeFor[int]()}
			if ownership == "linked row URI" {
				id.Tag = `codec:"structql,uri=sql/order_keys.sql"`
			}
			require.NoError(t, os.WriteFile(filepath.Join(dir, "row.go"), []byte("package orders\ntype Row struct{ Id int `"+string(id.Tag)+"` }\n"), 0644))
			const pkg = "example.com/generated/orders"
			catalog := typecatalog.NewCatalog()
			row := x.NewType(reflect.StructOf([]reflect.StructField{id}), x.WithName("Row"), x.WithPkgPath(pkg))
			require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, row))
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
			require.NoError(t, err)
			keys := &spec.Parameter{Name: "OrderKeys", TypeExpr: "string", Source: spec.BindSource{Kind: "query", Name: "keys"}, Codec: &spec.Codec{Body: "structql", Args: []string{"SELECT Id FROM `/`"}}}
			component := &spec.Component{
				Key: spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Orders"}, Name: "Orders",
				Parameters: []*spec.Parameter{keys, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}, TypeExpr: "[]*" + pkg + ".Row"}},
				RootView:   &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: "SELECT Id FROM orders"}, Columns: []*spec.Column{{Name: "Id", Source: "Id", Type: spec.TypeRef{Name: "int"}}}},
			}
			input := Input{nativeMutationTargetSelected: true, Component: component, TargetPackage: pkg, PackageName: "orders", ProjectRoot: root, TypeResolver: resolver, SQLResources: true, Views: ViewReferences{RootViewPath: {DescriptorKey: row.Key()}}}
			emit := func() *ResourcePlan {
				p, err := New(input).Plan()
				require.NoError(t, err)
				_, err = EmitScaffold(dir, p)
				require.NoError(t, err)
				return p.Resources
			}
			first := emit()
			const path = "sql/order_keys.sql"
			require.FileExists(t, filepath.Join(dir, path))
			if ownership == "other filesystem" {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "extra.go"), []byte("package orders\nimport \"embed\"\n//go:embed "+path+"\nvar extra embed.FS\n"), 0644))
			}
			if ownership == "field URI" {
				component.Parameters = append(component.Parameters, &spec.Parameter{Name: "StillUsed", TypeExpr: "string", Source: spec.BindSource{Kind: "query", Name: "reference"}, Codec: &spec.Codec{Body: "structql", Args: []string{"uri=" + first.Namespace + ":" + path}}})
			}
			keys.Codec = &spec.Codec{Body: "OrderKeyCodec"}
			for pass := 0; pass < 2; pass++ {
				last := emit()
				found := false
				for _, asset := range last.Files {
					if asset.Path == path {
						found = true
					}
				}
				wantInBundle := ownership == "field URI" || ownership == "linked row URI"
				require.Equal(t, wantInBundle, found)
				if ownership == "retired" {
					require.NoFileExists(t, filepath.Join(dir, path))
				} else {
					require.FileExists(t, filepath.Join(dir, path))
				}
				src, err := os.ReadFile(filepath.Join(dir, last.Destination))
				require.NoError(t, err)
				require.Equal(t, wantInBundle, strings.Contains(string(src), path))
			}
		})
	}
}
