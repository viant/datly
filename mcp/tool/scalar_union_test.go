package tool

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
	"testing"
)

type unionToolRow struct {
	Fee *choice.ScalarValue `json:"fee" choice:"4237:AD_SERVING" choice:"4241:AD_SERVING" enumAccept:"scalar" enumStyle:"scalar" enumInteger:"passthrough" enumUnknownString:"preserve"`
}
type unionToolBody struct {
	Rows []*unionToolRow `json:"rows"`
}
type unionToolInput struct {
	Body *unionToolBody `parameter:"Body,kind=body"`
}

func TestScalarUnionNestedMCPContract(t *testing.T) {
	binding := bindly.BindingSpec{Path: "Body", Name: "Body", Location: bindstate.Location{Kind: "body"}}
	contract := testRouteContract(t, reflect.TypeFor[unionToolInput](), []bindly.BindingSpec{binding})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "FeeUnion"}, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "feeUnion"}, Contract: contract})
	require.NoError(t, err)
	for _, value := range []any{int64(4237), int64(4241), int64(9007199254740993), "AD_SERVING", "unknown", nil} {
		args := map[string]any{"body": map[string]any{"rows": []any{map[string]any{"fee": value}}}}
		result, err := gojsonschema.Validate(gojsonschema.NewGoLoader(plan.Metadata().InputSchema), gojsonschema.NewGoLoader(args))
		require.NoError(t, err)
		require.True(t, result.Valid(), "value=%v: %v", value, result.Errors())
		scope, err := plan.Scope(args)
		require.NoError(t, err)
		body, _, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*unionToolBody](), "")
		scope.Close()
		require.NoError(t, err)
		actual := body.(*unionToolBody).Rows[0].Fee
		switch v := value.(type) {
		case int64:
			require.NotNil(t, actual)
			require.NotNil(t, actual.Integer)
			require.Equal(t, v, *actual.Integer)
			require.Nil(t, actual.String)
		case string:
			require.NotNil(t, actual)
			require.NotNil(t, actual.String)
			require.Equal(t, v, *actual.String)
			require.Nil(t, actual.Integer)
		default:
			require.Nil(t, actual)
		}
	}
}
