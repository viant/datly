package generate

import (
	"encoding/json"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	xshape "github.com/viant/x/shape"
)

func TestNestedColumnParserThroughEmitter(t *testing.T) {
	imports := []spec.ImportSpec{{Alias: "model", Package: "example.com/a/types"}, {Alias: "other", Package: "example.com/b/types"}}
	context := &spec.TypeContext{Imports: imports}
	authority := map[string]string{"model": "example.com/a/types", "other": "example.com/b/types"}
	for _, expression := range []string{"[][]int", "[][]*model.T", "[]*[]model.T", "*[]*[]*model.T", "map[int][][]*other.T", "[][]map[model.Key]*other.T", "[]map[string][][]*model.T"} {
		for _, collision := range []bool{false, true} {
			t.Run(expression+map[bool]string{false: "", true: " collision"}[collision], func(t *testing.T) {
				ref, err := dql.ColumnType(expression, context)
				require.NoError(t, err)
				encoded, err := json.Marshal(ref)
				require.NoError(t, err)
				var restored spec.TypeRef
				require.NoError(t, json.Unmarshal(encoded, &restored))
				require.Equal(t, ref, restored)
				plannedImports := append([]spec.ImportSpec(nil), imports...)
				if collision {
					plannedImports = []spec.ImportSpec{{Alias: "types", Package: "example.com/unrelated/types"}}
				}
				plan := &Plan{Package: "example.com/app", Imports: plannedImports}
				view := &spec.View{Name: "Values", Columns: []*spec.Column{{Name: "Value", Type: restored, ExplicitType: true, Nullable: true}}}
				fields, err := resolveScalarViewFields(plan, view, false)
				require.NoError(t, err)
				require.Len(t, fields, 1)
				emittedImports := map[string]string{}
				for _, item := range plan.Imports {
					emittedImports[item.Alias] = item.Package
				}
				actual, err := (xshape.Resolver{Package: plan.Package, Imports: emittedImports}).Canonical(fields[0].Type)
				require.NoError(t, err)
				expected, err := (xshape.Resolver{Imports: authority}).Canonical(expression)
				require.NoError(t, err)
				require.Equal(t, expected, actual)
				source := structFileWithImports("app", "", "Values", fields, plan.Imports)
				_, err = parser.ParseFile(token.NewFileSet(), "values.go", source, 0)
				require.NoError(t, err)
			})
		}
	}
}
