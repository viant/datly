package http

import (
	"context"
	"fmt"
	"mime"
	stdhttp "net/http"
	"strconv"
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
	format, err := h.outputFormatWithDefault(request, contract)
	if err != nil {
		code := xresponse.ErrorStatusCode(err, stdhttp.StatusBadRequest)
		writeJSON(writer, code, xresponse.Status{Status: "error", Message: err.Error(), Error: err.Error()})
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
	return h.outputFormatWithDefault(request, contract)
}

func (h *Handler) outputFormatWithDefault(request *stdhttp.Request, contract *output.Plan) (string, error) {
	format := contract.DefaultFormat()
	if route, ok := h.runtime.RouteByMethodPath(request.Method, request.URL.EscapedPath()); ok && route.Marshaller != "" {
		format = route.Marshaller
	}
	source, explicit := contract.FormatSelector()
	if !explicit {
		source.Kind, source.Name = "query", "_format"
	}
	if source.Kind == "header" {
		selected, err := selectAcceptedFormat(request.Header.Get(source.Name), format, contract)
		if err != nil {
			return "", &xresponse.Error{Code: stdhttp.StatusNotAcceptable, Cause: err}
		}
		format = selected
	} else if selected := strings.TrimSpace(request.URL.Query().Get(source.Name)); selected != "" {
		format = selected
	}
	format = strings.ToLower(strings.TrimSpace(format))
	if _, err := output.ContentType(format); err != nil {
		return "", &xresponse.Error{Code: stdhttp.StatusBadRequest, Cause: err}
	}
	if contract.JSONOnly() && format != "json" {
		return "", &xresponse.Error{Code: stdhttp.StatusNotAcceptable, Cause: fmt.Errorf("custom JSON output has no safe %s representation", format)}
	}
	return format, nil
}

type acceptedFormat struct {
	media       string
	quality     float64
	order       int
	specificity int
}

func selectAcceptedFormat(header, fallback string, contract *output.Plan) (string, error) {
	if strings.TrimSpace(header) == "" {
		return fallback, nil
	}
	if len(header) > 8192 {
		return "", fmt.Errorf("Accept header is too long")
	}
	parts := strings.Split(header, ",")
	if len(parts) > 32 {
		return "", fmt.Errorf("Accept header has too many media ranges")
	}
	ranges := make([]acceptedFormat, 0, len(parts))
	for index, part := range parts {
		media, params, err := mime.ParseMediaType(strings.TrimSpace(part))
		if err != nil {
			continue
		}
		quality := 1.0
		if raw := params["q"]; raw != "" {
			quality, err = strconv.ParseFloat(raw, 64)
			if err != nil || quality < 0 || quality > 1 {
				continue
			}
		}
		specificity := 2
		if media == "*/*" {
			specificity = 0
		} else if strings.HasSuffix(media, "/*") {
			specificity = 1
		}
		ranges = append(ranges, acceptedFormat{media: strings.ToLower(media), quality: quality, order: index, specificity: specificity})
	}
	candidates := []string{fallback, "json", "csv", "xml", "xlsx"}
	bestFormat, bestQuality, bestOrder := "", -1.0, len(parts)
	seen := map[string]bool{}
	for _, format := range candidates {
		format = strings.ToLower(strings.TrimSpace(format))
		if seen[format] {
			continue
		}
		seen[format] = true
		if format != "json" {
			if _, err := contract.Wire(format); err != nil {
				continue
			}
		}
		media, err := output.ContentType(format)
		if err != nil {
			continue
		}
		quality, order, specificity := -1.0, len(parts), -1
		for _, item := range ranges {
			matches := item.media == media || item.media == "*/*" ||
				strings.HasSuffix(item.media, "/*") && strings.HasPrefix(media, strings.TrimSuffix(item.media, "*"))
			if !matches {
				continue
			}
			if item.specificity > specificity || item.specificity == specificity && (item.quality > quality || item.quality == quality && item.order < order) {
				quality, order, specificity = item.quality, item.order, item.specificity
			}
		}
		if quality > 0 && (quality > bestQuality || quality == bestQuality && order < bestOrder) {
			bestFormat, bestQuality, bestOrder = format, quality, order
		}
	}
	if bestFormat == "" {
		return "", fmt.Errorf("no supported response media type was accepted")
	}
	return bestFormat, nil
}
