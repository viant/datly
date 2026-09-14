package openapi

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// ServeOptions distinguishes original fixed-YAML per-path documents from the
// aggregate's JSON default and explicit-format/Accept negotiation.
type ServeOptions struct {
	DefaultFormat string
	Negotiate     bool
}

type Handler struct {
	snapshot *Snapshot
	options  ServeOptions
}

func NewHandler(ctx context.Context, request Request) (*Handler, error) {
	snapshot, err := NewSnapshot(ctx, request)
	if err != nil {
		return nil, err
	}
	return snapshot.Handler(ServeOptions{DefaultFormat: "json", Negotiate: true})
}

func (s *Snapshot) Handler(options ServeOptions) (*Handler, error) {
	if s == nil {
		return nil, fmt.Errorf("OpenAPI snapshot is required")
	}
	switch options.DefaultFormat {
	case "":
		options.DefaultFormat = "json"
	case "json", "yaml":
	default:
		return nil, fmt.Errorf("invalid default document format %q", options.DefaultFormat)
	}
	return &Handler{snapshot: s, options: options}, nil
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if h == nil || h.snapshot == nil {
		writer.WriteHeader(500)
		return
	}
	if request.Method != "GET" && request.Method != "HEAD" {
		writer.Header().Set("Allow", "GET, HEAD")
		writer.WriteHeader(405)
		return
	}
	format := h.options.DefaultFormat
	if h.options.Negotiate {
		writer.Header().Add("Vary", "Accept")
		switch strings.ToLower(strings.TrimSpace(request.URL.Query().Get("format"))) {
		case "yaml", "yml":
			format = "yaml"
		case "json":
			format = "json"
		default:
			if strings.Contains(strings.ToLower(request.Header.Get("Accept")), "yaml") {
				format = "yaml"
			}
		}
	}
	data, contentType := h.snapshot.json, "application/json"
	if format == "yaml" {
		data, contentType = h.snapshot.yaml, "text/yaml"
	}
	writer.Header().Set("Content-Type", contentType)
	writer.Header().Set("Content-Length", strconv.Itoa(len(data)))
	writer.WriteHeader(http.StatusOK)
	if request.Method == "GET" {
		_, _ = writer.Write(data)
	}
}
