package output

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type mcpMethodOutput struct {
	*RepresentationEmbedded
	Secret string `json:"secret" mcp:"-"`
}

func TestMCPVisibilityPreservesLinkedMethods(t *testing.T) {
	original := reflect.TypeFor[mcpMethodOutput]()
	base, err := (Compiler{}).Compile(CompileInput{Type: original})
	require.NoError(t, err)
	selected, err := base.WithMCPVisibility()
	require.NoError(t, err)
	value := &mcpMethodOutput{RepresentationEmbedded: &RepresentationEmbedded{Public: "visible"}, Secret: "private"}
	encoded, err := selected.Encode(t.Context(), "json", value)
	require.NoError(t, err)
	require.JSONEq(t, `{"public":"visible"}`, string(encoded.Data))
	require.Equal(t, "private", value.Secret)
	require.Equal(t, "linked", value.Identity())
	require.Equal(t, original, selected.Type())
	unchanged, err := base.Encode(t.Context(), "json", value)
	require.NoError(t, err)
	require.JSONEq(t, `{"public":"visible","secret":"private"}`, string(unchanged.Data))
}
