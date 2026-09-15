package http

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"
	"net/url"

	"github.com/viant/bindly/provider/values"
)

// snapshot retains only declared authentication header values and routing/peer
// facts. No caller context, body, forms, trailers, TLS session or URL credentials
// enter server-owned lifetime. Unverified values are used only to authorize.
func (w *warmupRoutes) snapshot(ctx context.Context, req *stdhttp.Request, endpoint *warmupRoute) *stdhttp.Request {
	headers := stdhttp.Header{}
	names := append([]string(nil), w.policy.AdminHeaders...)
	names = append(names, endpoint.credentials...)
	names = append(names, endpoint.apiKeyHeader)
	for _, name := range names {
		if name != "" {
			if values := req.Header.Values(name); len(values) > 0 {
				headers[stdhttp.CanonicalHeaderKey(name)] = append([]string(nil), values...)
			}
		}
	}
	request := &stdhttp.Request{Method: req.Method, URL: &url.URL{Path: req.URL.Path, RawPath: req.URL.RawPath}, Header: headers, Host: req.Host, RemoteAddr: req.RemoteAddr, Body: stdhttp.NoBody}
	request.RequestURI = request.URL.RequestURI()
	return request.WithContext(ctx)
}

func (e *warmupRoute) execute(ctx context.Context, policy WarmupConfig, req *stdhttp.Request) (WarmupResult, int, error) {
	result := WarmupResult{Target: e.target.Route.String(), Status: "rejected"}
	if e.apiKeyHeader != "" && !(APIKey{Value: e.apiKeyValue}).matchesValue(req.Header.Get(e.apiKeyHeader)) {
		return result, 403, fmt.Errorf("component API key denied")
	}
	headers := map[string]any{}
	for _, name := range e.credentials {
		if value := req.Header.Get(name); value != "" {
			headers[name] = value
		}
	}
	if err := policy.Authorize(ctx, req, e.target); err != nil {
		return result, warmupErrorStatus(ctx, 403), err
	}
	if err := ctx.Err(); err != nil {
		return result, warmupErrorStatus(ctx, 500), err
	}
	operation, err := e.operation.Prepare(ctx, values.New("header", headers))
	if err != nil {
		return result, warmupErrorStatus(ctx, classifyRequestError(err)), err
	}
	if err := ctx.Err(); err != nil {
		return result, warmupErrorStatus(ctx, 500), err
	}
	result.Status = "ok"
	result.Groups, err = operation.Run(ctx)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		result.Status = "error"
		return result, warmupErrorStatus(ctx, 500), err
	}
	return result, 200, nil
}

func warmupErrorStatus(ctx context.Context, fallback int) int {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return stdhttp.StatusGatewayTimeout
	}
	if ctx.Err() != nil {
		return stdhttp.StatusServiceUnavailable
	}
	return fallback
}
