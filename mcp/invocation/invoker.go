// Package invocation adapts MCP calls to the protocol-neutral component port.
package invocation

import (
	"context"
	"strings"

	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/exec"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	xexec "github.com/viant/xdatly/exec"
	xmcp "github.com/viant/xdatly/handler/mcp"
	"github.com/viant/xdatly/response"
)

// OutputEncoder applies a compiled presentation contract without exposing its
// runtime owner to the protocol-neutral invocation package.
type OutputEncoder func(context.Context, any) ([]byte, error)

// ErrorProjector encodes an eligible declared error envelope without changing execution.
type ErrorProjector func(context.Context, int, error) ([]byte, bool, error)

type Config struct {
	Invoker   exec.ComponentInvoker
	Client    xmcp.Client
	Authorize func(context.Context, exec.ComponentTarget) error
	// Output selects the component's compiled JSON presentation adapter.
	Output func(exec.ComponentTarget) OutputEncoder
	// ErrorOutput selects a compiled adapter for explicit public error bodies.
	ErrorOutput          func(exec.ComponentTarget) OutputEncoder
	DeclaredError        func(exec.ComponentTarget) ErrorProjector
	SelectorPresentation func(context.Context, exec.ComponentTarget) (*exec.SelectorPresentation, error)
}

type Request struct {
	// PrepareScope selects a caller-effective argument mapping after tool
	// authorization. The transport retains ownership of closing the result.
	PrepareScope func(*exec.EffectiveContract) (*requestprovider.Scope, error)
	Target       exec.ComponentTarget
	Scope        *requestprovider.Scope
	Method       string
	URI          string
}

type Invoker struct {
	component            exec.ComponentInvoker
	mcp                  xmcp.Context
	authorize            func(context.Context, exec.ComponentTarget) error
	output               func(exec.ComponentTarget) OutputEncoder
	errorOutput          func(exec.ComponentTarget) OutputEncoder
	declaredError        func(exec.ComponentTarget) ErrorProjector
	selectorPresentation func(context.Context, exec.ComponentTarget) (*exec.SelectorPresentation, error)
}

func New(config Config) *Invoker {
	return &Invoker{component: config.Invoker, mcp: &requestContext{client: config.Client}, authorize: config.Authorize, output: config.Output, errorOutput: config.ErrorOutput, declaredError: config.DeclaredError, selectorPresentation: config.SelectorPresentation}
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
		if err := i.authorize(ctx, request.Target); err != nil {
			return nil, jsonrpc.NewInvalidRequest("MCP tool authorization denied", nil)
		}
	}
	ctx = exec.CaptureOutputSelection(ctx)
	ctx = exec.CaptureContracts(ctx)
	execContext := newExecutionContext(ctx, request)
	ctx = xexec.WithContext(ctx, execContext)
	ctx = WithMCPContext(ctx, i.mcp.Client())
	scope := request.Scope
	if request.PrepareScope != nil {
		var effective *exec.EffectiveContract
		if resolver, ok := i.component.(exec.ContractResolver); ok {
			authScope := AuthorizationScope(ctx)
			var err error
			effective, err = resolver.ResolveContract(ctx, request.Target, authScope)
			authScope.Close()
			if err != nil {
				if _, declared := response.ErrorBody(err); declared {
					var encodeError OutputEncoder
					if i.errorOutput != nil {
						encodeError = i.errorOutput(request.Target)
					}
					return &Execution{err: err, context: execContext, encodingContext: ctx, encodeErrorOutput: encodeError}, nil
				}
				return nil, jsonrpc.NewInvalidRequest("MCP contract authorization or preparation failed", nil)
			}
			exec.PublishContract(ctx, request.Target, effective)
		}
		var err error
		scope, err = request.PrepareScope(effective)
		if err != nil {
			return nil, jsonrpc.NewInvalidParamsError(err.Error(), nil)
		}
	}
	if header := authorizationHeader(ctx); header != "" {
		scope = scope.WithHeader("Authorization", header)
	}
	providers, err := requestProviders(ctx, scope)
	if err != nil {
		return nil, jsonrpc.NewInternalError("MCP request provider preparation failed", nil)
	}
	var presentation *exec.SelectorPresentation
	if i.selectorPresentation != nil {
		presentation, err = i.selectorPresentation(ctx, request.Target)
		if err != nil {
			return nil, jsonrpc.NewInternalError("MCP selector presentation preparation failed", nil)
		}
	}
	result, err := i.component.InvokeComponent(ctx, exec.ComponentRequest{
		Target: request.Target, Providers: providers, SelectorPresentation: presentation,
	})
	var encode OutputEncoder
	if i.output != nil {
		encode = i.output(request.Target)
	}
	var encodeError OutputEncoder
	if i.errorOutput != nil {
		encodeError = i.errorOutput(request.Target)
	}
	var projectError ErrorProjector
	if i.declaredError != nil {
		projectError = i.declaredError(request.Target)
	}
	return &Execution{projectError: projectError, encodeErrorOutput: encodeError, value: result, err: err, context: execContext, selection: exec.SelectedOutputFields(ctx, result), encodeOutput: encode, encodingContext: ctx}, nil
}

func newExecutionContext(ctx context.Context, request Request) *xexec.Context {
	result := xexec.New(xexec.WithMethod(request.Method), xexec.WithURI(request.URI), xexec.WithTraceResource("datly", ""))
	// Only server-supplied context carries trusted correlation. Never reuse its
	// mutable execution/trace state or derive correlation from tool arguments.
	if parent := xexec.GetContext(ctx); parent != nil {
		traceID := parent.TraceID
		if traceID == "" && parent.Trace != nil {
			traceID = parent.Trace.TraceID
		}
		if traceID != "" {
			result.TraceID = traceID
			result.Trace.TraceID = traceID
		}
	}
	return result
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

// AuthorizationScope uses the same verified protocol credential as tool calls.
// Metadata has no business arguments; request headers and the verified
// credential are projected through the same native binder as tool execution.
func AuthorizationScope(ctx context.Context) *requestprovider.Scope {
	return requestprovider.NewValues(requestprovider.WithHeaders(RequestHeaders(ctx)))
}

type requestContext struct {
	client xmcp.Client
}

func (c *requestContext) Client() xmcp.Client { return c.client }
