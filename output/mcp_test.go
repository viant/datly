package output

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type mcpStorageRow struct {
	ID string `json:"id" mcp:"-"`
}
type mcpDeclaredWire struct {
	ID string `json:"id"`
}
type mcpCustomOutput struct{ Data mcpStorageRow }

func (o *mcpCustomOutput) MarshalJSON() ([]byte, error) {
	return json.Marshal(mcpDeclaredWire{ID: o.Data.ID})
}
func (*mcpCustomOutput) JSONWireType() reflect.Type { return reflect.TypeFor[mcpDeclaredWire]() }

func TestMCPVisibilityUsesCustomEncoderDeclaredWire(t *testing.T) {
	base, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[mcpCustomOutput]()})
	require.NoError(t, err)
	selected, err := base.WithMCPVisibility()
	require.NoError(t, err)
	encoded, err := selected.Encode(t.Context(), "json", &mcpCustomOutput{Data: mcpStorageRow{ID: "created-id"}})
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"created-id"}`, string(encoded.Data))
	wire, err := selected.Wire("json")
	require.NoError(t, err)
	require.Equal(t, reflect.TypeFor[mcpDeclaredWire](), wire.Type)
}

type mcpHiddenWire struct {
	Secret string `json:"secret" mcp:"-"`
}
type mcpUnsafeCustomOutput struct{}

func (*mcpUnsafeCustomOutput) MarshalJSON() ([]byte, error) {
	return []byte(`{"secret":"private"}`), nil
}
func (*mcpUnsafeCustomOutput) JSONWireType() reflect.Type { return reflect.TypeFor[mcpHiddenWire]() }

func TestMCPVisibilityRejectsUnprojectedCustomWireExclusion(t *testing.T) {
	base, err := (Compiler{}).Compile(CompileInput{Type: reflect.TypeFor[mcpUnsafeCustomOutput]()})
	require.NoError(t, err)
	_, err = base.WithMCPVisibility()
	require.ErrorContains(t, err, "requires an MCP codec projection")
}

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
