package http

import (
	"encoding/json"
	"io"
	stdhttp "net/http"
	"strconv"
	"strings"

	xexec "github.com/viant/xdatly/exec"
	xresponse "github.com/viant/xdatly/response"
)

func classifyRequestError(err error) int {
	if err == nil {
		return stdhttp.StatusOK
	}
	if code := xresponse.ErrorStatusCode(err, 0); code != 0 {
		return code
	}
	msg := err.Error()
	switch {
	case strings.HasPrefix(msg, "unsupported content type "):
		return stdhttp.StatusUnsupportedMediaType
	case strings.HasPrefix(msg, "invalid content type "),
		strings.HasPrefix(msg, "missing required "),
		strings.Contains(msg, "failed to parse "),
		strings.HasPrefix(msg, "unsupported query bind kind "),
		strings.HasPrefix(msg, "failed to decode body "),
		strings.HasPrefix(msg, "failed to unmarshal body "),
		strings.HasPrefix(msg, "failed to unmarshal body field "):
		return stdhttp.StatusBadRequest
	default:
		return stdhttp.StatusInternalServerError
	}
}

func writeResponse(writer stdhttp.ResponseWriter, statusCode int, explicitStatus bool, response xresponse.Response) {
	if writer == nil || response == nil {
		return
	}
	if size := response.Size(); size > 0 && writer.Header().Get("Content-Length") == "" {
		writer.Header().Set("Content-Length", strconv.Itoa(size))
	}
	for key, values := range response.Headers() {
		writer.Header().Del(key)
		for _, value := range values {
			writer.Header().Add(key, value)
		}
	}
	if compressed, ok := response.(xresponse.Compressed); ok && compressed.CompressionType() != "" {
		writer.Header().Set("Content-Encoding", compressed.CompressionType())
	}
	if !explicitStatus {
		statusCode = response.StatusCode()
	}
	if statusCode == 0 {
		statusCode = stdhttp.StatusOK
	}
	writer.WriteHeader(responseStatusCode(statusCode))
	if body := response.Body(); body != nil {
		_, _ = io.Copy(writer, body)
	}
}

// responseStatusCode guards net/http's three-digit status requirement. Invalid
// application status values must not panic the transport or alter public bodies.
func responseStatusCode(code int) int {
	if code < 100 || code > 999 {
		return stdhttp.StatusInternalServerError
	}
	return code
}

func hasExplicitStatusCode(execCtx *xexec.Context, execErr error) bool {
	if execErr != nil {
		return true
	}
	return execCtx != nil && execCtx.StatusCode != 0 && execCtx.StatusCode != stdhttp.StatusOK
}

func publishMetricsHeaders(writer stdhttp.ResponseWriter, req *stdhttp.Request, execCtx *xexec.Context) {
	if writer == nil || req == nil || execCtx == nil {
		return
	}
	mode := req.Header.Get(datlyRequestMetricsHeader)
	if mode == "" {
		return
	}
	metrics := execCtx.Metrics
	if mode != datlyDebugHeaderValue {
		metrics = metrics.HideMetrics()
	}
	for _, metric := range metrics {
		if metric == nil {
			continue
		}
		data, err := json.Marshal(metric)
		if err != nil {
			continue
		}
		writer.Header().Add(datlyResponseHeaderMetrics+"-"+metric.Name(), string(data))
	}
}
