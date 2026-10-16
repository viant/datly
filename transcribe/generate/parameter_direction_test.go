package generate

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type directionRequest struct{ Ids []int }
type directionResult struct{ Id int }

func TestRuntimeContractsPreserveSameNamedInputAndOutput(t *testing.T) {
	for _, outputFirst := range []bool{false, true} {
		name := "input-first"
		if outputFirst {
			name = "output-first"
		}
		t.Run(name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[directionRequest](), x.WithName("Request"), x.WithPkgPath("example.com/models"))))
			require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[directionResult](), x.WithName("Result"), x.WithPkgPath("example.com/models"))))
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.TranscribeAuthority, &typecatalog.ResolutionContext{Imports: []typecatalog.PackageImport{{Alias: "models", Package: "example.com/models"}}})
			require.NoError(t, err)
			input := &spec.Parameter{Name: "Data", TypeExpr: "*models.Request", Source: spec.BindSource{Kind: "body", Name: "data"}}
			output := &spec.Parameter{Name: "Data", TypeExpr: "[]*models.Result", Source: spec.BindSource{Kind: "output", Name: "body"}}
			params := []*spec.Parameter{input, output}
			if outputFirst {
				params = []*spec.Parameter{output, input}
			}
			component := &spec.Component{Name: "Directional", TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "models", Package: "example.com/models"}}}, Parameters: params}
			generator := New(Input{Component: component, TypeResolver: resolver, TargetPackage: "example.com/generated"})
			inputType, err := generator.RuntimeInputType()
			require.NoError(t, err)
			field, ok := inputType.FieldByName("Data")
			require.True(t, ok)
			require.Equal(t, reflect.TypeFor[*directionRequest](), field.Type)
			outputType, err := generator.RuntimeOutputType()
			require.NoError(t, err)
			field, ok = outputType.FieldByName("Data")
			require.True(t, ok)
			require.Equal(t, reflect.TypeFor[[]*directionResult](), field.Type)
		})
	}
}
