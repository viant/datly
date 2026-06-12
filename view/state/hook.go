package state

import (
	"context"

	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/handler/http"
	"github.com/viant/xdatly/handler/state"
)

// Initializer is an interface that should be implemented by any type that needs to be initialized
type Initializer interface {
	Init(ctx context.Context) error
}

// Finalizer is an interface that should be implemented by any type that needs to be finalized
type Finalizer interface {
	Finalize(ctx context.Context) error
}

// FinaliserWithError is an error-aware finalizer that receives an error from previous steps.
type FinalizerWithError interface {
	Finalize(ctx context.Context, err error) error
}

type InjectorFinalizer interface {
	Finalize(ctx context.Context, getInjector func(ctx context.Context, path http.Route) (state.Injector, error)) error
}

// MCPClient exposes the MCP client-side capabilities that a server-side output
// may use during MCP-specific finalization.
type MCPClient interface {
	CanElicit() bool
	CanGenerateContent() bool
	Elicit(ctx context.Context, params *schema.ElicitRequestParams) (*schema.ElicitResult, error)
	GenerateContent(ctx context.Context, params *schema.CreateMessageRequestParams) (*schema.CreateMessageResult, error)
}

// MCPContext carries MCP-specific runtime capabilities for output
// finalization. The concrete implementation is supplied by the MCP host
// runtime.
type MCPContext interface {
	Client() MCPClient
}

// MCPFinalizer is an MCP-aware finalizer that runs only when MCP context is
// available on the current request path.
type MCPFinalizer interface {
	FinalizeMCP(ctx context.Context, mcp MCPContext) error
}

type mcpContextKey struct{}

// WithMCPContext attaches MCP-specific runtime context to ctx.
func WithMCPContext(ctx context.Context, mcp MCPContext) context.Context {
	return context.WithValue(ctx, mcpContextKey{}, mcp)
}

// LookupMCPContext extracts MCP-specific runtime context from ctx.
func LookupMCPContext(ctx context.Context) (MCPContext, bool) {
	if ctx == nil {
		return nil, false
	}
	value := ctx.Value(mcpContextKey{})
	if value == nil {
		return nil, false
	}
	mcp, ok := value.(MCPContext)
	return mcp, ok && mcp != nil
}
