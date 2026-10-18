package transcribe

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	smodel "github.com/viant/x/syntetic/model"
)

type outputAliasTarget struct{ Message string }

func TestExplicitOutputContractAliasIdentity(t *testing.T) {
	const pkg = "example.com/outputaliases"
	for _, tc := range []struct {
		name, declaration, source, want string
	}{
		{"alias", "[]*aliases.Alias", "type Alias = target.outputAliasTarget", ""},
		{"qualified alias", "[]*" + pkg + ".Alias", "type Alias = target.outputAliasTarget", ""},
		{"alias chain", "[]*aliases.Alias", "type Alias = Next; type Next = target.outputAliasTarget", ""},
		{"alias wrapper", "aliases.Alias", "type Alias = []*target.outputAliasTarget", ""},
		{"defined type", "[]*aliases.Alias", "type Alias target.outputAliasTarget", "differs from linked field"},
		{"pointer arity", "[]aliases.Alias", "type Alias = target.outputAliasTarget", "differs from linked field"},
		{"slice arity", "*aliases.Alias", "type Alias = target.outputAliasTarget", "differs from linked field"},
		{"cycle", "[]*aliases.Alias", "type Alias = Next; type Next = Alias", "cyclic output type alias"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output := reflect.StructOf([]reflect.StructField{{Name: "Items", Type: reflect.TypeFor[[]*outputAliasTarget](), Tag: `parameter:"Items,kind=output,in=body"`}})
			catalog := typecatalog.NewCatalog()
			file, err := parser.ParseFile(token.NewFileSet(), "aliases.go", "package aliases; "+tc.source, 0)
			require.NoError(t, err)
			var declarations []*smodel.Type
			for _, decl := range file.Decls {
				for _, spec := range decl.(*ast.GenDecl).Specs {
					declaration := spec.(*ast.TypeSpec)
					declarations = append(declarations, &smodel.Type{Name: declaration.Name.Name, PkgPath: pkg, TypeSpec: declaration, Imports: map[string]*smodel.ImportRef{"target": {Path: reflect.TypeFor[outputAliasTarget]().PkgPath()}}})
				}
			}
			require.NoError(t, catalog.RegisterPackage(typecatalog.TypeOriginPackage, &smodel.Package{PkgPath: pkg, Types: declarations}))
			require.NoError(t, catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, x.NewType(output, x.WithPkgPath(pkg), x.WithName("Output"))))
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "aliases", Package: pkg}}})
			require.NoError(t, err)
			param := &spec.Parameter{Name: "Items", TypeExpr: tc.declaration, Source: spec.BindSource{Kind: "output", Name: "body"}}
			result := &Result{Component: &spec.Component{Parameters: []*spec.Parameter{param}}, TypeResolver: resolver, Views: gen.ViewReferences{gen.RootViewPath: {DescriptorKey: "example.com/rows.Row"}}}
			err = linkExplicitOutputContract(result, "aliases.Output", nil)
			if tc.want == "" {
				require.NoError(t, err)
				require.Equal(t, pkg+".Output", result.Contracts.Output.DescriptorKey)
			} else {
				require.ErrorContains(t, err, tc.want)
				require.Nil(t, result.Contracts.Output)
			}
			require.Equal(t, tc.declaration, param.TypeExpr)
			require.Equal(t, "example.com/rows.Row", result.Views[gen.RootViewPath].DescriptorKey)
		})
	}
}
