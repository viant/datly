package http

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	stdhttp "net/http"
	"strconv"

	"github.com/viant/datly/spec"
)

func (h *Handler) responseCompression(ctx context.Context, request *stdhttp.Request) *spec.ResponseCompression {
	if h == nil || h.runtime == nil || request == nil {
		return nil
	}
	return h.runtime.ResponseCompressionByRoute(request.Method, request.URL.EscapedPath())
}

// writeEncodedBytes applies component policy only to framework-encoded output.
// Explicit response objects keep ownership of headers, streams and encoding.
// Like the original policy, gzip does not negotiate Accept-Encoding.
func writeEncodedBytes(writer stdhttp.ResponseWriter, request *stdhttp.Request, status int, data []byte, policy *spec.ResponseCompression) error {
	if policy != nil {
		if err := policy.Validate(); err != nil {
			return err
		}
		if len(data) > policy.MinSizeBytes && writer.Header().Get("Content-Encoding") == "" {
			var buffer bytes.Buffer
			compressor := gzip.NewWriter(&buffer)
			if _, err := compressor.Write(data); err != nil {
				return err
			}
			if err := compressor.Close(); err != nil {
				return err
			}
			data = buffer.Bytes()
			writer.Header().Set("Content-Encoding", "gzip")
		}
		writer.Header().Set("Content-Length", strconv.Itoa(len(data)))
	}
	if request != nil && request.Method == stdhttp.MethodHead && writer.Header().Get("Content-Length") == "" {
		writer.Header().Set("Content-Length", strconv.Itoa(len(data)))
	}
	writer.WriteHeader(responseStatusCode(status))
	if request == nil || request.Method != stdhttp.MethodHead {
		_, _ = writer.Write(data)
	}
	return nil
}

func (h *Handler) writeHTTPJSON(ctx context.Context, writer stdhttp.ResponseWriter, request *stdhttp.Request, status int, payload any) {
	policy := h.responseCompression(ctx, request)
	if policy == nil {
		recordHTTPError(ctx, writeJSON(writer, status, payload))
		return
	}
	var data []byte
	if payload != nil {
		var err error
		data, err = json.Marshal(payload)
		if err != nil {
			h.writeOutputError(ctx, writer, err)
			return
		}
	}
	writer.Header().Set("Content-Type", "application/json")
	if err := writeEncodedBytes(writer, request, status, data, policy); err != nil {
		h.writeOutputError(ctx, writer, err)
	}
}
