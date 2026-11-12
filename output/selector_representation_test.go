package output

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestSelectorRepresentationBelongsToExactRow(t *testing.T) {
	type publicRow struct {
		ID       int
		Category string `mcp:"name=iabCategory"`
		Secret   string `mcp:"-"`
	}
	type response struct{ Data []*publicRow }
	type currentRow struct {
		ID       int
		Category string
		Other    string
		Fourth   int
	}
	base, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[response](), Component: &spec.Component{Settings: &spec.Settings{CaseFormat: "lc"}}})
	require.NoError(t, err)
	plan, err := base.WithMCPVisibility()
	require.NoError(t, err)
	fields, err := plan.SelectorFieldNames(reflect.TypeFor[publicRow](), "Data")
	require.NoError(t, err)
	require.Len(t, fields, 2)
	require.Equal(t, "iabCategory", fields[1].PublicName)
	// Internal Current reads share the component but not the output row contract.
	for _, path := range []string{"", "Data"} {
		fields, err = plan.SelectorFieldNames(reflect.TypeFor[currentRow](), path)
		require.NoError(t, err)
		require.Nil(t, fields, "unrelated row must not receive output presentation")
	}
}
