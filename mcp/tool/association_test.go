package tool

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/x/shape"
	"github.com/viant/xdatly/handler"
)

type associationTestLink struct {
	Id         int
	CreativeId *int64 `mcp:"memberKey"`
}
type associationTestOrder struct {
	Creatives []*associationTestLink `association:"collection=creativeIds,member=CreativeId"`
}
type associationTestBody struct {
	Orders []*associationTestOrder
}
type associationTestInput struct {
	Body *associationTestBody
}
type associationTestAnonymous struct {
	Body *associationTestOrder `anonymous:"true"`
}

func compileAssociationTest(t *testing.T, typ reflect.Type) (*Plan, error) {
	t.Helper()
	contract := testRouteContract(t, typ, []bindly.BindingSpec{{Path: "Body", Location: state.Location{Kind: "body"}}})
	return NewCompiler(WithStrictArguments(true)).Compile(Input{Component: spec.Key{Kind: spec.KindComponent, Scope: "test", Name: "associationTest"}, Contract: contract, Exposure: &spec.MCPExposure{Name: "associationTest", Kind: spec.MCPExposureTool}})
}

func TestAssociationNamedBody(t *testing.T) {
	plan, err := compileAssociationTest(t, reflect.TypeFor[associationTestInput]())
	require.NoError(t, err)
	props := schemaObjectNode(plan.Metadata().InputSchema.Properties["body"])["properties"].(map[string]any)
	orderProps := schemaObjectNode(props["orders"].(map[string]any))["properties"].(map[string]any)
	require.NotContains(t, orderProps, "creatives")
	require.Contains(t, orderProps, "creativeIdsToBeAdded")
	require.Contains(t, orderProps, "creativeIdsToBeDeleted")
	member := orderProps["creativeIdsToBeAdded"].(map[string]any)["items"].(map[string]any)
	require.Equal(t, "integer", member["type"])
	adds := []any{json.Number("9223372036854775807"), json.Number("9223372036854775807")}
	order := map[string]any{"creativeIdsToBeAdded": adds, "creativeIdsToBeDeleted": []any{float64(5)}}
	args := map[string]any{"body": map[string]any{"orders": []any{order, map[string]any{"creativeIdsToBeDeleted": []any{}}, map[string]any{}}}}
	ctx, scope, err := plan.AssociationScope(t.Context(), args)
	require.NoError(t, err)
	defer scope.Close()
	value, found, err := scope.Body().Locate(nil).Value(ctx, reflect.TypeFor[*associationTestBody](), "")
	require.NoError(t, err)
	require.True(t, found)
	body := value.(*associationTestBody)
	require.Len(t, body.Orders[0].Creatives, 2)
	require.Equal(t, int64(9223372036854775807), *body.Orders[0].Creatives[0].CreativeId)
	require.Equal(t, int64(5), *body.Orders[0].Creatives[1].CreativeId)
	require.Equal(t, 0, body.Orders[0].Creatives[1].Id)
	target := handler.ComponentTarget{Scope: plan.target.Component.Scope, Name: plan.target.Component.Name, Method: plan.target.Route.Method, Path: plan.target.Route.Path}
	holder := reflect.TypeFor[associationTestOrder]()
	delta, ok := handler.LookupAssociationDelta(ctx, target, holder, "Creatives", "Body.Orders[0].Creatives")
	require.True(t, ok)
	require.Equal(t, 1, delta.AddedCount())
	require.True(t, delta.IsDeleted(int64(5)))
	require.True(t, delta.IsAdded(int64(9223372036854775807)))
	delta, ok = handler.LookupAssociationDelta(ctx, target, holder, "Creatives", "Body.Orders[1].Creatives")
	require.True(t, ok)
	require.Zero(t, delta.AddedCount()+delta.DeletedCount())
	_, ok = handler.LookupAssociationDelta(ctx, target, holder, "Creatives", "Body.Orders[2].Creatives")
	require.False(t, ok)
	target.Path += "/sibling"
	_, ok = handler.LookupAssociationDelta(ctx, target, holder, "Creatives", "Body.Orders[0].Creatives")
	require.False(t, ok)
	require.NotContains(t, order, "creatives")
	require.Equal(t, adds, order["creativeIdsToBeAdded"])
}

func TestAssociationAnonymousAdmission(t *testing.T) {
	plan, err := compileAssociationTest(t, reflect.TypeFor[associationTestAnonymous]())
	require.NoError(t, err)
	require.Contains(t, plan.Metadata().InputSchema.Properties, "creativeIdsToBeDeleted")
	for _, args := range []map[string]any{
		{"creativeIdsToBeAdded": []any{1}, "creativeIdsToBeDeleted": []any{1}},
		{"creativeIdsToBeDeleted": []any{1}, "creatives": nil},
		{"creatives": []any{}},
		{"Creatives": []any{}},
		{"creativeIdsToBeAdded": nil},
		{"creativeIdsToBeAdded": []any{nil}},
		{"creativeIdsToBeAdded": []any{"1"}},
		{"creativeIdsToBeAdded": []any{1.5}},
		{"creativeIdsToBeAdded": []any{float64(1 << 53)}},
		{"creativeIdsToBeDeleted": []any{json.Number("9223372036854775808")}},
	} {
		_, scope, err := plan.AssociationScope(t.Context(), args)
		require.Error(t, err, "%#v", args)
		require.Nil(t, scope)
	}
	_, err = plan.Scope(map[string]any{"creativeIdsToBeAdded": []int{3}})
	require.ErrorContains(t, err, "AssociationScope")
	_, scope, err := plan.AssociationScope(t.Context(), map[string]any{"creativeIdsToBeAdded": []int{3}})
	require.NoError(t, err)
	defer scope.Close()
	value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[*associationTestOrder](), "")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(3), *value.(*associationTestOrder).Creatives[0].CreativeId)
}

func TestAssociationEffectiveMemberVisibility(t *testing.T) {
	plan, err := compileAssociationTest(t, reflect.TypeFor[associationTestInput]())
	require.NoError(t, err)
	walker, err := shape.CompileRewrite(plan.input.Type())
	require.NoError(t, err)
	repr, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if parent == reflect.TypeFor[associationTestLink]() && field.Name == "CreativeId" {
			return &shape.FieldMetadata{Name: field.Name, Tag: `json:"-" internal:"true"`}, nil
		}
		return nil, nil
	})
	require.NoError(t, err)
	input := plan.compilerInput
	input.Fields, err = input.Contract.MetadataRepresentation(repr, nil)
	require.NoError(t, err)
	hidden, err := NewCompiler(WithStrictArguments(true)).Compile(input)
	require.NoError(t, err)
	require.Empty(t, hidden.associations)
}

func TestAssociationCompanionCollision(t *testing.T) {
	type conflict struct {
		Creatives []*associationTestLink `association:"collection=creativeIds,member=CreativeId"`
		External  string                 `mcp:"aliases=creativeIdsToBeAdded"`
	}
	type input struct{ Body *conflict }
	_, err := compileAssociationTest(t, reflect.TypeFor[input]())
	require.ErrorContains(t, err, "creativeIdsToBeAdded")
}

func TestAssociationNestedAliasesCannotShadowTranslatedRows(t *testing.T) {
	plan, err := compileAssociationTest(t, reflect.TypeFor[associationTestInput]())
	require.NoError(t, err)
	for _, body := range []any{
		map[string]any{"orders": []any{map[string]any{"CREATIVES": []any{}, "creativeIdsToBeDeleted": []any{3}}}},
		map[string]any{"Orders": []any{map[string]any{"creativeIdsToBeAdded": []any{3}}}, "orders": []any{map[string]any{}}},
	} {
		_, _, err = plan.AssociationScope(t.Context(), map[string]any{"body": body})
		require.Error(t, err)
	}
}

func TestAssociationSerializableIntent(t *testing.T) {
	plan, err := compileAssociationTest(t, reflect.TypeFor[associationTestAnonymous]())
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		args map[string]any
		want exec.TransactionIsolation
	}{
		{"ordinary", map[string]any{}, exec.IsolationReadCommitted},
		{"edits", map[string]any{"creativeIdsToBeDeleted": []int{3}}, exec.IsolationSerializable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := exec.WithTransactionIsolation(t.Context(), exec.IsolationReadCommitted)
			ctx, scope, err := plan.AssociationScope(parent, tc.args)
			require.NoError(t, err)
			defer scope.Close()
			got, ok := exec.RequestedTransactionIsolation(ctx)
			require.True(t, ok)
			require.Equal(t, tc.want, got)

			called := false
			service := NewHandler(plan, invocation.New(invocation.Config{Invoker: binaryComponentFunc(func(ctx context.Context, target exec.ComponentRequest) (any, error) {
				called = true
				got, ok := exec.RequestedTransactionIsolation(ctx)
				require.True(t, ok)
				require.Equal(t, tc.want, got)
				return map[string]any{"ok": true}, nil
			})}))
			req := &schema.CallToolRequest{}
			req.Params.Name = plan.Metadata().Name
			req.Params.Arguments = tc.args
			_, rpcErr := service.Handle(parent, req)
			require.Nil(t, rpcErr)
			require.True(t, called)
		})
	}
}
