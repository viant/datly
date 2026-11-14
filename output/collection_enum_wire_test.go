package output

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

type collectionWireRow struct {
	Codes    []int    `json:"codes" mcp:"choices" mcpEnum:"true" choice:"7:ALPHA" choice:"3:BETA" enumAccept:"literal" enumStyle:"literal" enumNull:"reject"`
	Labels   []string `json:"labels" mcpEnum:"true" choice:"ALPHA" choice:"BETA" enumAccept:"literal"`
	Secret   []int    `json:"secret" mcp:"-"`
	Hidden   []int    `json:"-"`
	Internal []int    `json:"internal" internal:"true"`
}

// The caller's MCP policy is represented by the existing metadata rewrite;
// neither serialization nor the storage DTO acquires a business policy owner.
func TestCollectionEnumNativeWireAndExport(t *testing.T) {
	value := []collectionWireRow{{Codes: []int{7, 3, 7}, Labels: []string{"BETA", "ALPHA"}, Secret: []int{99}, Hidden: []int{98}, Internal: []int{97}}, {Codes: []int{}, Labels: []string{}}, {}}
	typ := reflect.TypeOf(value)
	base, err := (Compiler{}).Compile(CompileInput{Type: typ, Component: &spec.Component{Settings: &spec.Settings{Output: &spec.OutputSettings{Exclude: []string{"Internal"}}}}})
	require.NoError(t, err)
	mcp, err := base.WithMCPVisibility()
	require.NoError(t, err)
	walk, err := xshape.CompileRewrite(typ)
	require.NoError(t, err)
	storage, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Tag.Get("mcpEnum") != "true" {
			return nil, nil
		}
		field := (&xcontract.Field{Name: f.Name, Tag: f.Tag}).RemoveTag("choice").RemoveTag("enumAccept").RemoveTag("enumStyle").RemoveTag("enumNull")
		copy := *f
		copy.Tag = field.Tag
		return &copy, nil
	})
	require.NoError(t, err)
	rest, err := base.WithRepresentation(storage)
	require.NoError(t, err)
	before, err := json.Marshal(value)
	require.NoError(t, err)
	for repeat := 0; repeat < 2; repeat++ {
		encoded, err := mcp.Encode(t.Context(), "json", value)
		require.NoError(t, err)
		require.JSONEq(t, `[{"choices":["ALPHA","BETA","ALPHA"],"labels":["BETA","ALPHA"]},{"choices":[],"labels":[]},{"choices":null,"labels":null}]`, string(encoded.Data))
		stored, err := rest.Encode(t.Context(), "json", value)
		require.NoError(t, err)
		require.JSONEq(t, `[{"codes":[7,3,7],"labels":["BETA","ALPHA"],"secret":[99]},{"codes":[],"labels":[],"secret":null},{"codes":null,"labels":null,"secret":null}]`, string(stored.Data))
	}
	for _, format := range []string{"csv", "xls", "xlsx"} {
		t.Run(format, func(t *testing.T) {
			encoded, err := mcp.Encode(t.Context(), format, value)
			require.NoError(t, err)
			// The existing presentation uses the compiled enum owner for collection cells.
			require.Equal(t, [][]string{{"Codes", "Labels"}, {"[ALPHA BETA ALPHA]", "[BETA ALPHA]"}, {"[]", "[]"}, {"[]", "[]"}}, scalarExportRows(t, encoded, format))
			stored, err := rest.Encode(t.Context(), format, value)
			require.NoError(t, err)
			require.Equal(t, [][]string{{"Codes", "Labels", "Secret"}, {"[7 3 7]", "[BETA ALPHA]", "[99]"}, {"[]", "[]", "[]"}, {"[]", "[]", "[]"}}, scalarExportRows(t, stored, format))
		})
	}
	invalid := []collectionWireRow{{Codes: []int{7, 99}}}
	for _, format := range []string{"json", "csv", "xls", "xlsx"} {
		_, err = mcp.Encode(t.Context(), format, invalid)
		require.ErrorContains(t, err, "enum collection item 1: unknown enum ordinal 99")
	}
	// The precompiled Tagly owner distinguishes input null rejection from output.
	field, _ := reflect.TypeFor[collectionWireRow]().FieldByName("Codes")
	descriptor, err := choice.ParseCollectionEnum(field.Tag, field.Type)
	require.NoError(t, err)
	_, err = descriptor.DecodeJSON([]byte("null"))
	require.ErrorIs(t, err, choice.ErrNullInput)
	nullOutput, err := descriptor.Encode([]int(nil))
	require.NoError(t, err)
	require.Nil(t, nullOutput)
	after, err := json.Marshal(value)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after))
}
