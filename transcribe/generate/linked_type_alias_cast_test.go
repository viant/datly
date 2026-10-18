package generate

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	smodel "github.com/viant/x/syntetic/model"
)

type linkedAliasDimension struct{ Name string }
type linkedAliasView struct {
	Dimensions *[]*linkedAliasDimension `sqlx:"DIMENSIONS"`
}

func TestLinkedCASTGoAliasIdentity(t *testing.T) {
	const pkg = "example.com/audience"
	for _, tc := range []struct{ name, expression, source, failure string }{
		{"pointer to slice alias", "*audience.Dimensions", "type Dimensions = []*target.linkedAliasDimension", ""},
		{"local alias", "*Dimensions", "type Dimensions = []*target.linkedAliasDimension", ""},
		{"qualified alias", "*" + pkg + ".Dimensions", "type Dimensions = []*target.linkedAliasDimension", ""},
		{"alias chain", "*audience.Dimensions", "type Dimensions = Next; type Next = []*target.linkedAliasDimension", ""},
		{"defined slice", "*audience.Dimensions", "type Dimensions []*target.linkedAliasDimension", "CAST cannot change"},
		{"missing pointer", "audience.Dimensions", "type Dimensions = []*target.linkedAliasDimension", "CAST cannot change"},
		{"wrong element pointer", "*audience.Dimensions", "type Dimensions = []target.linkedAliasDimension", "CAST cannot change"},
		{"cycle", "*audience.Dimensions", "type Dimensions = Next; type Next = Dimensions", "cyclic linked type alias"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, err := parser.ParseFile(token.NewFileSet(), "aliases.go", "package audience; "+tc.source, 0)
			require.NoError(t, err)
			var declarations []*smodel.Type
			for _, decl := range file.Decls {
				for _, item := range decl.(*ast.GenDecl).Specs {
					declaration := item.(*ast.TypeSpec)
					declarations = append(declarations, &smodel.Type{Name: declaration.Name.Name, PkgPath: pkg, TypeSpec: declaration, Imports: map[string]*smodel.ImportRef{"target": {Path: reflect.TypeFor[linkedAliasDimension]().PkgPath()}}})
				}
			}
			catalog := typecatalog.NewCatalog()
			require.NoError(t, catalog.RegisterPackage(typecatalog.TypeOriginPackage, &smodel.Package{PkgPath: pkg, Types: declarations}))
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, nil)
			require.NoError(t, err)
			view := &spec.View{Name: "DefaultAudience", Columns: []*spec.Column{{Name: "DIMENSIONS", ExplicitType: true, Type: spec.TypeRef{Name: tc.expression}}}}
			before, err := json.Marshal(view)
			require.NoError(t, err)
			validator := &linkedViewCastValidator{plan: &Plan{Package: pkg, Imports: []spec.ImportSpec{{Alias: "audience", Package: pkg}}}, shape: xshape.New(x.NewType(reflect.TypeFor[linkedAliasView]()), resolver.Descriptor), lookup: resolver.Descriptor, visiting: map[*spec.View]bool{}}
			err = validator.validate(view, "")
			if tc.failure == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.failure)
			}
			after, err := json.Marshal(view)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after))
		})
	}
}
