package gateway

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/mcp-protocol/schema"
)

func selectorTestParameter(name, bound string, typ reflect.Type) *state.Parameter {
	return state.NewParameter(name, state.NewQueryLocation(bound), state.WithParameterSchema(state.NewSchema(typ)))
}

func TestMCPRelationSelectorsDiscoverAndRouteWithoutChangingRoot(t *testing.T) {
	child := &view.ReferenceView{View: view.View{Name: "physical_matches", Selector: &view.Config{
		LimitParameter:   selectorTestParameter("Limit", "match_limit", reflect.TypeOf(0)),
		OffsetParameter:  selectorTestParameter("Offset", "match_offset", reflect.TypeOf(0)),
		FieldsParameter:  selectorTestParameter("Fields", "match_fields", reflect.TypeOf([]string{})),
		OrderByParameter: selectorTestParameter("OrderBy", "match_order", reflect.TypeOf([]string{})),
		PageParameter:    selectorTestParameter("Page", "match_page", reflect.TypeOf(0)),
	}}}
	child.With = []*view.Relation{{Holder: "Details", Of: &view.ReferenceView{View: view.View{Name: "details", Selector: &view.Config{
		LimitParameter: selectorTestParameter("Limit", "detail_limit", reflect.TypeOf(0)),
	}}}}}
	component := &repository.Component{View: &view.View{Name: "parent", Selector: &view.Config{Constraints: &view.Constraints{Limit: true, Offset: true, Projection: true}}, With: []*view.Relation{{Name: "different_sql_alias", Holder: "Match", Of: child}}}}
	// A selector also declared in business input must not swallow its public,
	// relation-scoped name or publish two controls for the same HTTP binding.
	component.Input.Type.Parameters = state.Parameters{selectorTestParameter("InternalChildLimit", "match_limit", reflect.TypeOf(0))}
	router := &Router{}
	typ := router.buildToolInputType(component)
	var discovery schema.ToolInputSchema
	require.NoError(t, discovery.Load(reflect.New(typ).Interface()))
	require.Equal(t, "integer", discovery.Properties["MatchLimit"]["type"])
	require.Equal(t, "array", discovery.Properties["MatchFields"]["type"])
	require.NotContains(t, discovery.Required, "MatchLimit")
	for _, name := range []string{"Limit", "Offset", "Fields", "MatchLimit", "MatchOffset", "MatchFields", "MatchOrderBy", "MatchPage", "MatchDetailsLimit"} {
		field, ok := typ.FieldByName(name)
		require.True(t, ok, name)
		require.Equal(t, "true", field.Tag.Get("optional"), name)
	}
	_, duplicate := typ.FieldByName("InternalChildLimit")
	require.False(t, duplicate)
	var actual url.Values
	route := &Route{Path: &contract.Path{Method: http.MethodGet, URI: "/selectors"}, Handler: func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		actual = r.URL.Query()
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}}
	_, rpcErr := router.mcpToolCallHandler(component, route)(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Arguments: map[string]interface{}{
		"Limit": 11.0, "Offset": 22.0, "Fields": []string{"id", "match"},
		"MatchLimit": 3.0, "MatchOffset": 6.0, "MatchPage": 2.0, "MatchFields": []interface{}{"id", "campaign_id"}, "MatchOrderBy": []string{"id:desc"}, "MatchDetailsLimit": 1.0,
	}}})
	require.Nil(t, rpcErr)
	require.Equal(t, "11", actual.Get("_limit"))
	require.Equal(t, "22", actual.Get("_offset"))
	require.Equal(t, "id,match", actual.Get("_fields"))
	require.Equal(t, "3", actual.Get("match_limit"))
	require.Equal(t, "6", actual.Get("match_offset"))
	require.Equal(t, "2", actual.Get("match_page"))
	require.Equal(t, "id,campaign_id", actual.Get("match_fields"))
	require.Equal(t, "id:desc", actual.Get("match_order"))
	require.Equal(t, "1", actual.Get("detail_limit"))
	// The normal HTTP session consumes the same declared child binding.
	ns := &view.NamespaceView{View: &child.View}
	bound := ns.SelectorParameters(child.Selector.LimitParameter, view.QueryStateParameters.LimitParameter)
	require.Len(t, bound, 1)
	require.Equal(t, "3", actual.Get(bound[0].In.Name))
	require.Equal(t, "Limit", child.Selector.LimitParameter.Name, "discovery must not mutate the runtime definition")
	_, rpcErr = router.mcpToolCallHandler(component, route)(context.Background(), &schema.CallToolRequest{Params: schema.CallToolRequestParams{Arguments: map[string]interface{}{"MatchLimit": 4.0}}})
	require.Nil(t, rpcErr)
	require.Equal(t, "4", actual.Get("match_limit"))
	require.Empty(t, actual.Get("_limit"), "child-only argument must not page the parent")
	require.Empty(t, actual.Get("limit"), "child-only argument must not send root alias")
}

func TestMCPSelectorDoesNotInventChildBindings(t *testing.T) {
	root := &view.View{Selector: &view.Config{Constraints: &view.Constraints{Limit: true}}, With: []*view.Relation{{Holder: "Match", Of: &view.ReferenceView{View: view.View{Selector: &view.Config{Constraints: &view.Constraints{Limit: true}}}}}}}
	params := mcpSelectorParameters(root)
	require.Len(t, params, 2)
	require.Equal(t, "Limit", params[0].Name)
	// An explicitly shared binding cannot silently relabel the root.
	root.With[0].Of.Selector.LimitParameter = selectorTestParameter("Limit", "_limit", reflect.TypeOf(0))
	component := &repository.Component{View: root}
	params = mcpComponentParameters(component)
	require.Len(t, params, 2)
	require.Equal(t, "Limit", params[0].Name)
}

func TestMCPQueryListTypedAndDecodedValues(t *testing.T) {
	for _, value := range []interface{}{[]string{"id", "name"}, []interface{}{"id", "name"}} {
		items, ok := mcpQueryList(value)
		require.True(t, ok)
		require.Equal(t, []string{"id", "name"}, items)
	}
}

func TestMCPSelectorDefaultInternalNamesHavePublicSchemaNames(t *testing.T) {
	parameter := selectorTestParameter("_limit", "_limit", reflect.TypeOf(0))
	component := &repository.Component{View: &view.View{Selector: &view.Config{LimitParameter: parameter}}}
	typ := (&Router{}).buildToolInputType(component)
	_, exists := typ.FieldByName("Limit")
	require.True(t, exists)
	require.Equal(t, "_limit", parameter.Name, "the HTTP definition must remain unchanged")
}
