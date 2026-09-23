// Package invocation adapts MCP calls to the protocol-neutral component port.
package invocation

import (
	"context"
	"strings"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/output"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	xexec "github.com/viant/xdatly/exec"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type Config struct {
	Invoker   exec.ComponentInvoker
	Client    xmcp.Client
	Authorize func(context.Context, exec.ComponentTarget) error
	// Output supplies the component's compiled JSON presentation contract.
	Output func(exec.ComponentTarget) *output.Plan
}

type Request struct {
	Target exec.ComponentTarget
	Scope  *requestprovider.Scope
	Method string
	URI    string
}

type Invoker struct {
	component exec.ComponentInvoker
	mcp       xmcp.Context
	authorize func(context.Context, exec.ComponentTarget) error
	output    func(exec.ComponentTarget) *output.Plan
}

func New(config Config) *Invoker {
	return &Invoker{component: config.Invoker, mcp: &requestContext{client: config.Client}, authorize: config.Authorize, output: config.Output}
}

func (i *Invoker) Execute(ctx context.Context, request Request) (*Execution, *jsonrpc.Error) {
	if i == nil || i.component == nil {
		return nil, jsonrpc.NewInternalError("MCP component invoker is unavailable", nil)
	}
	if request.Target.Component.Kind == "" || request.Target.Route.String() == "" {
		return nil, jsonrpc.NewInvalidParamsError("MCP component target is incomplete", nil)
	}
	if strings.TrimSpace(request.Method) == "" || strings.TrimSpace(request.URI) == "" {
		return nil, jsonrpc.NewInternalError("MCP invocation identity is incomplete", nil)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if i.authorize != nil {
		// The hook may bind trusted, server-owned providers for this exact
		// target; the same context must reach the component so they apply.
		ctx, _ = exec.CaptureScopeBinding(ctx)
		if err := i.authorize(ctx, request.Target); err != nil {
			return nil, jsonrpc.NewInvalidRequest("MCP tool authorization denied", nil)
		}
	}
	ctx = exec.CaptureOutputSelection(ctx)
	execContext := xexec.New(xexec.WithMethod(request.Method), xexec.WithURI(request.URI))
	ctx = xexec.WithContext(ctx, execContext)
	if _, ok := xmcp.LookupContext(ctx); !ok {
		ctx = xmcp.WithContext(ctx, i.mcp)
	}
	scope := request.Scope
	if header := authorizationHeader(ctx); header != "" {
		scope = scope.WithHeader("Authorization", header)
	}
	var providers []locator.Provider
	if scope != nil {
		providers = scope.Providers()
	}
	result, err := i.component.InvokeComponent(ctx, exec.ComponentRequest{
		Target: request.Target, Providers: providers,
	})
	var plan *output.Plan
	if i.output != nil {
		plan = i.output(request.Target)
	}
	return &Execution{value: result, err: err, context: execContext, selection: exec.SelectedOutputFields(ctx, result), output: plan, encodingContext: ctx}, nil
}

func authorizationHeader(ctx context.Context) string {
	var value string
	switch token := ctx.Value(authorization.TokenKey).(type) {
	case *authorization.Token:
		if token != nil {
			value = token.Token
		}
	case authorization.Token:
		value = token.Token
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(strings.Fields(value)) > 1 {
		return value
	}
	return "Bearer " + value
}

type requestContext struct {
	client xmcp.Client
}

func (c *requestContext) Client() xmcp.Client { return c.client }
