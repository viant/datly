// Package mcp compiles route exposures into an immutable Datly MCP catalog and
// a fresh upstream protocol registry.
package mcp

import (
	"context"
	"fmt"

	bindresource "github.com/viant/bindly/resource"
	"github.com/viant/datly/exec"
	mcpresource "github.com/viant/datly/mcp/resource"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	mcpserver "github.com/viant/mcp-protocol/server"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type Config struct {
	Folders         []mcpresource.Folder
	Components      []*registry.RegisteredComponent
	Invoker         exec.ComponentInvoker
	Client          xmcp.Client
	Resources       *bindresource.Store
	ResourceBaseURI string
	Authorization   *authorization.Policy
}

type Service struct {
	catalog   *Catalog
	registry  *mcpserver.Registry
	resources *mcpresource.Handler
	policy    *authorization.Policy
}

func (s *Service) ReadResource(ctx context.Context, request *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error) {
	if s == nil || s.resources == nil {
		return nil, jsonrpc.NewInternalError("MCP resource service is unavailable", nil)
	}
	return s.resources.Handle(ctx, request)
}

func New(config Config) (*Service, error) {
	return config.Compile(context.Background())
}

// Compile stages resource snapshots in the caller's generation context.
func (config Config) Compile(ctx context.Context) (*Service, error) {
	if ctx == nil {
		return nil, fmt.Errorf("MCP staging context is required")
	}
	return (&serviceCompiler{config: config}).Compile(ctx)
}

func (s *Service) Catalog() *Catalog {
	if s == nil {
		return nil
	}
	return s.catalog
}

func (s *Service) Registry() *mcpserver.Registry {
	if s == nil {
		return nil
	}
	return s.registry
}

// Authorization returns a detached copy of the validated transport policy.
func (s *Service) Authorization() *authorization.Policy {
	if s == nil {
		return nil
	}
	return cloneAuthorizationPolicy(s.policy)
}
