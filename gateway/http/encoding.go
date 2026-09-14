package http

import (
	"context"
	stdhttp "net/http"
	"strings"

	"github.com/viant/datly/runtime/output"
	xresponse "github.com/viant/xdatly/response"
)

func (h *Handler) writeEncoded(ctx context.Context, writer stdhttp.ResponseWriter, request *stdhttp.Request, status int, value any) {
	contract, ok := h.runtime.OutputByRoute(request.Method, request.URL.EscapedPath())
	if !ok {
		writeJSON(writer, status, value)
		return
	}
	format := h.outputFormat(request)
	if _, err := output.ContentType(format); err != nil {
		writeJSON(writer, stdhttp.StatusBadRequest, xresponse.Status{Status: "error", Message: err.Error(), Error: err.Error()})
		return
	}
	encoded, err := contract.Encode(ctx, format, value)
	if err != nil {
		if h.logger != nil {
			h.logger.Error("failed to encode HTTP output", err)
		}
		writer.Header().Del("Content-Length")
		writer.Header().Del("Content-Encoding")
		writeJSON(writer, stdhttp.StatusInternalServerError, xresponse.Status{Status: "error", Message: "internal server error", Error: "internal server error"})
		return
	}
	writer.Header().Set("Content-Type", encoded.ContentType)
	if encoded.ContentDisposition != "" {
		writer.Header().Set("Content-Disposition", encoded.ContentDisposition)
	}
	writer.WriteHeader(responseStatusCode(status))
	_, _ = writer.Write(encoded.Data)
}

// outputFormat is shared by encoding and async dispatch policy, so invalid or
// forced-synchronous formats are decided before a durable job is created.
func (h *Handler) outputFormat(request *stdhttp.Request) string {
	format := "json"
	if contract, ok := h.runtime.OutputByRoute(request.Method, request.URL.EscapedPath()); ok {
		format = contract.DefaultFormat()
	}
	if route, ok := h.runtime.RouteByMethodPath(request.Method, request.URL.EscapedPath()); ok && route.Marshaller != "" {
		format = route.Marshaller
	}
	if selected := strings.TrimSpace(request.URL.Query().Get("_format")); selected != "" {
		format = selected
	}
	return strings.ToLower(format)
}
