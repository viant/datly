package resource

import (
	"context"
	"errors"
	"io/fs"
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

type Handler struct {
	catalog *Catalog
	invoker *invocation.Invoker
}

func NewHandler(catalog *Catalog, invoker *invocation.Invoker) *Handler {
	return &Handler{catalog: catalog, invoker: invoker}
}

func (h *Handler) Handle(ctx context.Context, request *schema.ReadResourceRequest) (*schema.ReadResourceResult, *jsonrpc.Error) {
	if ctx == nil {
		return nil, jsonrpc.NewInvalidParamsError("resource context is required", nil)
	}
	if h == nil || h.catalog == nil {
		return nil, jsonrpc.NewInternalError("MCP resource handler is unavailable", nil)
	}
	if request == nil || strings.TrimSpace(request.Params.Uri) == "" {
		return nil, jsonrpc.NewInvalidParamsError("MCP resource URI is required", nil)
	}
	resolved, err := h.catalog.Resolve(request.Params.Uri)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, schema.NewResourceNotFound(request.Params.Uri)
		}
		return nil, resolutionError(err)
	}
	if file := resolved.plan.file; file != nil {
		if err := ctx.Err(); err != nil {
			return nil, jsonrpc.NewInternalError("resource read canceled", nil)
		}
		data, err := fs.ReadFile(file.source, file.name)
		if err != nil {
			return nil, schema.NewResourceNotFound(request.Params.Uri)
		}
		content := schema.ReadResourceResultContentsElem{Uri: resolved.URI(), MimeType: stringPointer(resolved.Plan().MIMEType())}
		if isTextMIME(resolved.Plan().MIMEType()) && utf8.Valid(data) {
			content.Text = string(data)
		} else {
			content.Blob = encodeBlob(data)
		}
		return &schema.ReadResourceResult{Contents: []schema.ReadResourceResultContentsElem{content}}, nil
	}
	if h.invoker == nil {
		return nil, jsonrpc.NewInternalError("resource invocation is unavailable", nil)
	}
	execution, protocolErr := h.invoker.Execute(ctx, invocation.Request{
		Target: resolved.Plan().Target(), Scope: resolved.Scope(),
		Method: schema.MethodResourcesRead, URI: resolved.URI(),
	})
	if protocolErr != nil {
		return nil, protocolErr
	}
	if rpcErr := executionError(execution); rpcErr != nil {
		return nil, rpcErr
	}
	return contentResult(resolved, execution)
}

func resolutionError(err error) *jsonrpc.Error {
	switch {
	case errors.Is(err, ErrNotFound):
		return jsonrpc.NewMethodNotFound(err.Error(), nil)
	case errors.Is(err, ErrInvalidURI):
		return jsonrpc.NewInvalidParamsError(err.Error(), nil)
	default:
		return jsonrpc.NewInternalError("resolve MCP resource", nil)
	}
}

func executionError(execution *invocation.Execution) *jsonrpc.Error {
	status := execution.StatusCode()
	if execution.Error() == nil && status < http.StatusBadRequest {
		return nil
	}
	message := exec.ErrorMessage(execution.Error(), status)
	if status < http.StatusInternalServerError {
		return jsonrpc.NewInvalidParamsError(message, nil)
	}
	return jsonrpc.NewInternalError(message, nil)
}

func contentResult(resolved *Resolved, execution *invocation.Execution) (*schema.ReadResourceResult, *jsonrpc.Error) {
	payload, err := execution.Payload()
	if err != nil {
		return nil, jsonrpc.NewInternalError("encode MCP resource", nil)
	}
	mimeType := resolved.Plan().MIMEType()
	content := schema.ReadResourceResultContentsElem{Uri: resolved.URI(), MimeType: stringPointer(mimeType)}
	if isTextMIME(mimeType) {
		content.Text = string(payload)
	} else {
		content.Blob = encodeBlob(payload)
	}
	return &schema.ReadResourceResult{Contents: []schema.ReadResourceResultContentsElem{content}}, nil
}
