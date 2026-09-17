package http

import (
	"context"
	stdhttp "net/http"
	"strings"

	"github.com/viant/datly/runtime/output"
	xresponse "github.com/viant/xdatly/response"
)

func (h *Handler) writeEncoded(ctx context.Context, writer stdhttp.ResponseWriter, request *stdhttp.Request, status int, value any) {
	contract, err := h.runtime.ResolveOutputByRoute(ctx, request.Method, request.URL.EscapedPath())
	if err != nil {
		h.writeOutputError(writer, err)
		return
	}
	format := h.outputFormatWithDefault(request, contract.DefaultFormat())
	if _, err := output.ContentType(format); err != nil {
		writeJSON(writer, stdhttp.StatusBadRequest, xresponse.Status{Status: "error", Message: err.Error(), Error: err.Error()})
		return
	}
	encoded, err := contract.Encode(ctx, format, value)
	if err != nil {
		h.writeOutputError(writer, err)
		return
	}
	writer.Header().Set("Content-Type", encoded.ContentType)
	if encoded.ContentDisposition != "" {
		writer.Header().Set("Content-Disposition", encoded.ContentDisposition)
	}
	writer.WriteHeader(responseStatusCode(status))
	_, _ = writer.Write(encoded.Data)
}

func (h *Handler) writeOutputError(writer stdhttp.ResponseWriter, err error) {
	if h.logger != nil {
		h.logger.Error("failed to encode HTTP output", err)
	}
	writer.Header().Del("Content-Length")
	writer.Header().Del("Content-Encoding")
	writeJSON(writer, stdhttp.StatusInternalServerError, xresponse.Status{Status: "error", Message: "internal server error", Error: "internal server error"})
}

// outputFormat is shared by encoding and async dispatch policy, so invalid or
// forced-synchronous formats are decided before a durable job is created.
func (h *Handler) outputFormat(ctx context.Context, request *stdhttp.Request) (string, error) {
	contract, err := h.runtime.ResolveOutputByRoute(ctx, request.Method, request.URL.EscapedPath())
	if err != nil {
		return "", err
	}
	return h.outputFormatWithDefault(request, contract.DefaultFormat()), nil
}

func (h *Handler) outputFormatWithDefault(request *stdhttp.Request, format string) string {
	if route, ok := h.runtime.RouteByMethodPath(request.Method, request.URL.EscapedPath()); ok && route.Marshaller != "" {
		format = route.Marshaller
	}
	if selected := strings.TrimSpace(request.URL.Query().Get("_format")); selected != "" {
		format = selected
	}
	return strings.ToLower(format)
}
