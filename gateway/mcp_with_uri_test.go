package gateway

import (
	"context"
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	dpath "github.com/viant/datly/repository/path"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	serverproto "github.com/viant/mcp-protocol/server"
)

func TestRouterBuildToolsIntegrationRegistersWithURIAlternate(t *testing.T) {
	id := state.NewParameter("Id", state.NewPathLocation("id"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf([]int{}))))
	id.URI = "/v1/api/things/{id}"
	search := state.NewParameter("Search", state.NewQueryLocation("search"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf(""))))
	component := &repository.Component{
		Path:     contract.Path{Method: http.MethodGet, URI: "/v1/api/things"},
		View:     &view.View{Name: "thing"},
		Contract: contract.Contract{Input: contract.Input{Type: state.Type{Parameters: state.Parameters{id, search}}}},
	}
	provider := repository.NewProvider(component.Path, &version.Control{}, func(context.Context, ...repository.Option) (*repository.Component, error) {
		return component, nil
	})
	base := &dpath.Path{
		Path:                 contract.Path{Method: http.MethodGet, URI: "/v1/api/things"},
		Meta:                 contract.Meta{Name: "Things", Description: "List things"},
		ModelContextProtocol: contract.ModelContextProtocol{MCPTool: true},
		View:                 &dpath.ViewRef{Ref: "thing"},
	}
	byID := &dpath.Path{
		Path:                 contract.Path{Method: http.MethodGet, URI: "/v1/api/things/{id}"},
		Meta:                 contract.Meta{Name: "Things", Description: "Get a thing"},
		ModelContextProtocol: contract.ModelContextProtocol{MCPTool: true},
		View:                 &dpath.ViewRef{Ref: "thing"},
	}
	item := &dpath.Item{Paths: []*dpath.Path{base, byID}}
	registry := serverproto.NewRegistry()
	router := &Router{mcpRegistry: registry}
	newRoute := func(path *dpath.Path) *Route {
		return &Route{Path: &path.Path, Handler: func(context.Context, http.ResponseWriter, *http.Request) {}}
	}

	require.NoError(t, router.buildToolsIntegration(item, base, newRoute(base), provider))
	require.NoError(t, router.buildToolsIntegration(item, byID, newRoute(byID), provider))

	tools := map[string]struct {
		properties map[string]map[string]interface{}
		required   []string
	}{}
	for _, tool := range registry.ListRegisteredTools() {
		tools[tool.Name] = struct {
			properties map[string]map[string]interface{}
			required   []string
		}{tool.InputSchema.Properties, tool.InputSchema.Required}
	}
	require.Len(t, tools, 2)
	require.Contains(t, tools, "Things")
	require.Contains(t, tools, "ThingsById")
	assert.NotContains(t, tools["Things"].properties, "Id", "base tool must not duplicate the WithURI route")
	assert.NotContains(t, tools["Things"].required, "Id")
	assert.Contains(t, tools["ThingsById"].properties, "Id")
	assert.Contains(t, tools["ThingsById"].required, "Id")
	assert.Contains(t, tools["ThingsById"].properties, "Search")
}

func TestRouterBuildToolsIntegrationCanHideWithURIAlternate(t *testing.T) {
	id := state.NewParameter("Id", state.NewPathLocation("id"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf([]int{}))))
	id.URI = "/v1/api/things/{id}"
	pathMCP := false
	id.PathMCP = &pathMCP
	component := &repository.Component{
		Path:     contract.Path{Method: http.MethodGet, URI: "/v1/api/things"},
		View:     &view.View{Name: "thing"},
		Contract: contract.Contract{Input: contract.Input{Type: state.Type{Parameters: state.Parameters{id}}}},
	}
	provider := repository.NewProvider(component.Path, &version.Control{}, func(context.Context, ...repository.Option) (*repository.Component, error) {
		return component, nil
	})
	base := &dpath.Path{Path: component.Path, Meta: contract.Meta{Name: "Things"}, ModelContextProtocol: contract.ModelContextProtocol{MCPTool: true}, View: &dpath.ViewRef{Ref: "thing"}}
	byID := &dpath.Path{Path: contract.Path{Method: http.MethodGet, URI: id.URI}, Meta: contract.Meta{Name: "Things"}, ModelContextProtocol: contract.ModelContextProtocol{MCPTool: true}, View: &dpath.ViewRef{Ref: "thing"}}
	item := &dpath.Item{Paths: []*dpath.Path{base, byID}}
	registry := serverproto.NewRegistry()
	router := &Router{mcpRegistry: registry}
	newRoute := func(path *dpath.Path) *Route {
		return &Route{Path: &path.Path, Handler: func(context.Context, http.ResponseWriter, *http.Request) {}}
	}

	require.NoError(t, router.buildToolsIntegration(item, base, newRoute(base), provider))
	require.NoError(t, router.buildToolsIntegration(item, byID, newRoute(byID), provider))
	require.Len(t, registry.ListRegisteredTools(), 1)
	assert.Equal(t, "Things", registry.ListRegisteredTools()[0].Name)
}

func TestMCPRouteControlsDefaultEnabledAndCanHideBase(t *testing.T) {
	id := state.NewParameter("Id", state.NewPathLocation("id"), state.WithParameterSchema(state.NewSchema(reflect.TypeOf([]int{}))))
	id.URI = "/v1/api/things/{id}"
	component := &repository.Component{Contract: contract.Contract{Input: contract.Input{Type: state.Type{Parameters: state.Parameters{id}}}}}
	base := &dpath.Path{Path: contract.Path{Method: http.MethodGet, URI: "/v1/api/things"}}
	byID := &dpath.Path{Path: contract.Path{Method: http.MethodGet, URI: id.URI}}

	assert.True(t, mcpRouteEnabled(component, base, false))
	assert.True(t, mcpRouteEnabled(component, byID, true))
	mainMCP := false
	id.MCP = &mainMCP
	assert.False(t, mcpRouteEnabled(component, base, false))
	assert.True(t, mcpRouteEnabled(component, byID, true))
}

func TestMCPAlternateRouteSuffix(t *testing.T) {
	assert.Equal(t, "ById", mcpAlternateRouteSuffix("/things", "/things/{id}"))
	assert.Equal(t, "ByCampaignId", mcpAlternateRouteSuffix("/advertisers/{advertiserId}", "/advertisers/{advertiserId}/{campaignId}"))
	assert.Equal(t, "ByThirdParty", mcpAlternateRouteSuffix("/providers", "/providers/third-party"))
}
