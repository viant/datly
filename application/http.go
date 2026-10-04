package application

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	gateway "github.com/viant/datly/gateway/http"
	internallog "github.com/viant/datly/internal/logging"
	xexec "github.com/viant/xdatly/exec"
)

func (m *Manager) newHTTPHandler(ctx context.Context, built *Build, input gateway.HandlerInput) (*gateway.Handler, error) {
	httpConfig := built.HTTP
	if httpConfig.Warmup != nil {
		policy := *httpConfig.Warmup
		if policy.Lifetime != nil {
			return nil, fmt.Errorf("application owns HTTP warmup lifetime")
		}
		policy.Lifetime = m.warmups
		httpConfig.Warmup = &policy
	}
	return httpConfig.Build(ctx, input)
}

func (m *Manager) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, current, release, err := m.pinOwned(r.Context())
	if err != nil {
		if m != nil && m.observation != nil && internallog.Enabled(m.observation.Recorder) {
			internallog.ServeHTTPObserved(w, r, "", func(w http.ResponseWriter, r *http.Request) {
				internallog.RecordHTTPError(r.Context(), err)
				http.Error(w, "application unavailable", http.StatusServiceUnavailable)
			}, func(ctx context.Context, e *xexec.Context) { internallog.LogHTTP(m.observation.Recorder, ctx, e) })
		} else {
			http.Error(w, "application unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	defer release()
	r = r.WithContext(ctx)
	if m.observation != nil && internallog.Enabled(m.observation.Recorder) {
		internallog.ServeHTTPObserved(w, r, current.version, func(w http.ResponseWriter, r *http.Request) { m.serveGenerationHTTP(current, w, r) }, func(ctx context.Context, e *xexec.Context) { internallog.LogHTTP(m.observation.Recorder, ctx, e) })
		return
	}
	m.serveGenerationHTTP(current, w, r)
}

func (m *Manager) serveGenerationHTTP(current *generation, w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if current.documents != nil {
		prefix := strings.TrimSuffix(current.httpConfig.Meta.OpenApiURI, "/")
		if prefix == "" {
			prefix = gateway.DefaultOpenAPIURI
		}
		docURI := strings.TrimSuffix(current.httpConfig.Meta.DocURI, "/")
		if docURI == "" {
			docURI = gateway.DefaultDocURI
		}
		path := r.URL.EscapedPath()
		if path == docURI || prefix != "" && (path == prefix || strings.HasPrefix(path, prefix+"/")) {
			if err := current.ensureDocuments(ctx); err != nil {
				internallog.RecordHTTPError(ctx, err)
				http.Error(w, "OpenAPI unavailable", http.StatusServiceUnavailable)
				return
			}
		}
	}
	current.httpMu.Lock()
	handler := current.http
	current.httpMu.Unlock()
	handler.ServeHTTP(w, r.WithContext(ctx))
}
