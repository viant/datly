package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository"
	"github.com/viant/datly/repository/contract"
	dpath "github.com/viant/datly/repository/path"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/shared/logging"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	serverproto "github.com/viant/mcp-protocol/server"
)

func TestBuildMCPIntegrationsSkipsUnavailableComponentAndRegistersHealthyTool(t *testing.T) {
	ctx := context.Background()
	registry := serverproto.NewRegistry()
	router := &Router{mcpRegistry: registry, logger: logging.New(logging.INFO, io.Discard)}
	toolPath := &dpath.Path{
		Path:                 contract.Path{Method: http.MethodGet, URI: "/v1/api/test/tool"},
		Meta:                 contract.Meta{Name: "HealthyTool", Description: "healthy tool"},
		ModelContextProtocol: contract.ModelContextProtocol{MCPTool: true},
		View:                 &dpath.ViewRef{Ref: "healthy"},
	}
	item := &dpath.Item{Paths: []*dpath.Path{toolPath}}
	route := &Route{Path: &toolPath.Path, Handler: func(context.Context, http.ResponseWriter, *http.Request) {}}
	failingProvider := repository.NewProvider(toolPath.Path, &version.Control{}, func(context.Context, ...repository.Option) (*repository.Component, error) {
		return nil, errors.New("datasource unavailable")
	})

	router.buildMCPIntegrations(ctx, item, toolPath, route, failingProvider)
	require.Empty(t, registry.ListRegisteredTools())

	component := &repository.Component{
		Path:     toolPath.Path,
		View:     &view.View{Name: "healthy"},
		Contract: contract.Contract{Input: contract.Input{Type: state.Type{}}},
	}
	healthyProvider := repository.NewProvider(toolPath.Path, &version.Control{}, func(context.Context, ...repository.Option) (*repository.Component, error) {
		return component, nil
	})
	router.buildMCPIntegrations(ctx, item, toolPath, route, healthyProvider)

	tools := registry.ListRegisteredTools()
	require.Len(t, tools, 1)
	require.Equal(t, "HealthyTool", tools[0].Name)
}
