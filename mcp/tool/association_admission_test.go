package tool

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/spec"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

// Both mechanisms must refuse the original input before reaching the component,
// including in the ordinary lenient MCP argument mode.
func TestAssociationDeltaOnlyHandlerAdmission(t *testing.T) {
	type row struct {
		Links  []*associationTestLink `json:"links" mcp:"name=objects,aliases=priorObjects" format:"inputNames=legacyLinks,inputNamesCase=fold" association:"collection=memberIds,member=CreativeId"`
		Ids    []int                  `json:"ids" format:"inputNames=oldIds,inputNamesCase=fold"`
		Remove []int                  `json:"removeIds" mcp:"name=remove,aliases=oldRemove"`
		Other  []int                  `json:"other"`
	}
	type input struct{ Body *row }
	contract := testRouteContract(t, reflect.TypeFor[input](), []bindly.BindingSpec{{Path: "Body", Location: state.Location{Kind: "body"}}})
	plan, err := NewCompiler().Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "deltaOnly"}, Contract: contract, Exposure: &spec.MCPExposure{Name: "updates", Kind: spec.MCPExposureTool, PrimitivePairs: []spec.MCPPrimitivePair{{Path: "body", Add: "ids", Remove: "remove"}}}})
	require.NoError(t, err)
	props := schemaObjectNode(plan.Metadata().InputSchema.Properties["body"])["properties"].(map[string]any)
	for _, companion := range []string{"memberIdsToBeAdded", "memberIdsToBeDeleted", "idsToBeAdded", "idsToBeDeleted", "other"} {
		require.Contains(t, props, companion)
	}
	called := 0
	service := NewHandler(plan, invocation.New(invocation.Config{Invoker: binaryComponentFunc(func(ctx context.Context, req exec.ComponentRequest) (any, error) {
		called++
		return map[string]any{"ok": true}, nil
	})}))
	call := func(args map[string]any) *jsonrpc.Error {
		req := &schema.CallToolRequest{}
		req.Params.Name = plan.Metadata().Name
		req.Params.Arguments = map[string]any{"body": args}
		_, rpcErr := service.Handle(t.Context(), req)
		return rpcErr
	}
	for _, hidden := range []string{"objects", "priorObjects", "links", "Links", "legacyLinks", "LEGACYLINKS", "ids", "Ids", "OLDIDS", "remove", "Remove", "oldRemove", "removeIds"} {
		t.Run("discovery/"+hidden, func(t *testing.T) {
			require.NotContains(t, props, hidden)
		})
		t.Run("admission/"+hidden, func(t *testing.T) {
			for _, args := range []map[string]any{{hidden: []any{}}, {hidden: nil, "idsToBeAdded": []any{2}}} {
				rpcErr := call(args)
				require.NotNil(t, rpcErr)
				require.EqualValues(t, -32602, rpcErr.Code)
				require.Contains(t, rpcErr.Message, hidden)
				require.Contains(t, rpcErr.Message, "is not public")
			}
		})
	}
	require.Zero(t, called)
	require.Nil(t, call(map[string]any{"memberIdsToBeDeleted": []any{2}, "idsToBeAdded": []any{1}, "other": []any{3}}))
	require.Equal(t, 1, called)
}

func TestAssociationDeltaOnlyCompanionFoldCollision(t *testing.T) {
	type row struct {
		Links []*associationTestLink `association:"collection=memberIds,member=CreativeId"`
		Other []int                  `json:"other" format:"inputNames=MEMBERIDSTOBEADDED,inputNamesCase=fold"`
	}
	type input struct{ Body *row }
	_, err := compileAssociationTest(t, reflect.TypeFor[input]())
	require.ErrorContains(t, err, "memberIdsToBeAdded")
}
