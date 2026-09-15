package http

import (
	"context"
	"encoding/json"
	"log"
	stdhttp "net/http"
	"strings"
	"time"

	requestprovider "github.com/viant/bindly/provider/request"
	dexec "github.com/viant/datly/exec"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	xexec "github.com/viant/xdatly/exec"
	xlogger "github.com/viant/xdatly/logger"
	xresponse "github.com/viant/xdatly/response"
)

const (
	datlyRequestMetricsHeader  = "Datly-Show-Metrics"
	datlyDebugHeaderValue      = "debug"
	datlyResponseHeaderMetrics = "Datly-Metrics"
	datlyServiceTimeHeader     = "Datly-Service-Time"
)

type Handler struct {
	metrics       *MetricsConfig
	async         *asyncRoutes
	allowedSubnet []string
	documents     *documentRoutes
	static        []*staticRoute
	cors          map[string]*corsPolicy
	warmup        *warmupRoutes
	runtime       *druntime.Runtime
	logger        xlogger.Logger
	version       string
}

func NewHandler(rt *druntime.Runtime, log xlogger.Logger, version string) *Handler {
	h := &Handler{
		runtime: rt,
		logger:  log,
		version: version,
		cors:    map[string]*corsPolicy{},
	}
	for _, endpoint := range rt.Routes() {
		policy, _ := newCORSPolicy(endpoint.CORS.Resolve(nil))
		h.cors[(spec.RouteRef{Method: endpoint.Method, Path: endpoint.Path}).String()] = policy
	}
	return h
}

func (h *Handler) ServeHTTP(writer stdhttp.ResponseWriter, req *stdhttp.Request) {
	if h == nil || h.runtime == nil {
		writer.WriteHeader(stdhttp.StatusInternalServerError)
		return
	}
	if !h.allowsRemote(req) {
		writer.WriteHeader(stdhttp.StatusForbidden)
		return
	}
	if h.documents != nil && h.serveDocuments(writer, req) {
		return
	}
	if h.warmup != nil && h.serveWarmup(writer, req) {
		return
	}
	if h.serveStatic(writer, req) {
		return
	}
	var handled bool
	writer, handled = h.serveCORS(writer, req)
	if handled {
		return
	}
	escapedPath := req.URL.EscapedPath()
	if methods := h.runtime.AllowedMethodsForPath(escapedPath); len(methods) == 0 {
		writeJSON(writer, stdhttp.StatusNotFound, xresponse.Status{
			Status:  "error",
			Message: "not found",
			Error:   "not found",
		})
		return
	} else if _, ok := h.runtime.RouteByMethodPath(req.Method, escapedPath); !ok {
		writer.Header().Set("Allow", strings.Join(methods, ", "))
		writeJSON(writer, stdhttp.StatusMethodNotAllowed, xresponse.Status{
			Status:  "error",
			Message: "method not allowed",
			Error:   "method not allowed",
		})
		return
	}
	if !h.canHandle(req) {
		writeJSON(writer, stdhttp.StatusForbidden, xresponse.Status{
			Status:  "error",
			Message: "forbidden",
			Error:   "forbidden",
		})
		return
	}
	started := time.Now()
	ctx := context.WithValue(req.Context(), xexec.ContextKey, xexec.NewContext(req.Method, req.RequestURI, req.Header, h.version))
	ctx = dexec.CaptureOutputSelection(ctx)
	asyncRoute := h.asyncRoute(req)
	var asyncService AsyncService
	if asyncRoute != nil {
		var release func()
		var err error
		ctx, asyncService, release, err = h.async.admission.Begin(ctx)
		if err != nil {
			writeJSON(writer, stdhttp.StatusServiceUnavailable, xresponse.Status{Status: "error", Message: err.Error(), Error: err.Error()})
			return
		}
		defer release()
		body := req.Body
		if body != nil {
			stop := context.AfterFunc(ctx, func() { _ = body.Close() })
			defer stop()
		}
	}
	pathParams, _ := h.runtime.MatchPathParams(req.Method, escapedPath)
	requestScope, scopeErr := requestprovider.New(req, requestprovider.WithPathParams(pathParams))
	if scopeErr != nil {
		if h.logger != nil {
			h.logger.Error("HTTP request preparation failed", scopeErr)
		} else {
			log.Printf("HTTP request preparation failed: %+v", scopeErr)
		}
		code := classifyRequestError(scopeErr)
		message := stdhttp.StatusText(code)
		writeJSON(writer, code, xresponse.Status{Status: "error", Message: message, Error: message})
		return
	}
	defer func() {
		if err := requestScope.Close(); err != nil && h.logger != nil {
			h.logger.Error("failed to clean up HTTP request scope", err)
		}
	}()
	var actual any
	var execErr error
	if asyncRoute != nil {
		asyncRoute, execErr = h.prepareAsyncRoute(req, asyncRoute)
		if execErr == nil {
			actual, execErr = asyncRoute.execute(ctx, req, requestScope, asyncService)
		}
	} else {
		actual, execErr = h.runtime.ExecuteRoute(ctx, req.Method, escapedPath, requestScope)
	}
	if h.logger != nil {
		if execErr != nil {
			h.logger.Error("HTTP route execution failed", execErr)
		} else {
			h.logger.Debug("HTTP route dispatched through handler engine")
		}
	} else if execErr != nil {
		log.Printf("HTTP route execution failed: %+v", execErr)
	}
	execCtx := xexec.GetContext(ctx)
	statusCode := stdhttp.StatusOK
	if execErr != nil {
		if explicit := xresponse.ErrorStatusCode(execErr, 0); explicit != 0 {
			statusCode = explicit
		} else if execCtx != nil && execCtx.StatusCode != 0 && execCtx.StatusCode != stdhttp.StatusInternalServerError {
			statusCode = execCtx.StatusCode
		} else {
			statusCode = classifyRequestError(execErr)
		}
	} else if execCtx != nil && execCtx.StatusCode != 0 {
		statusCode = execCtx.StatusCode
	}
	publicBody, hasPublicBody := xresponse.ErrorBody(execErr)
	if hasPublicBody {
		actual = publicBody
		if actual == nil {
			actual = json.RawMessage("null")
		}
	}
	if execErr != nil && !hasPublicBody {
		message := dexec.ErrorMessage(execErr, statusCode)
		actual = xresponse.Status{
			Status:  "error",
			Message: message,
			Error:   message,
		}
	}
	writer.Header().Set(datlyServiceTimeHeader, time.Since(started).String())
	h.publishMetricsHeaders(writer, req, execCtx)
	if response, ok := actual.(xresponse.Response); ok {
		writeResponse(writer, statusCode, hasExplicitStatusCode(execCtx, execErr), response)
		return
	}
	if execErr != nil {
		writeJSON(writer, statusCode, actual)
		return
	}
	h.writeEncoded(ctx, writer, req, statusCode, actual)
}

func (h *Handler) canHandle(req *stdhttp.Request) bool {
	if h == nil || h.runtime == nil || req == nil {
		return false
	}
	route, ok := h.runtime.RouteByMethodPath(req.Method, req.URL.EscapedPath())
	if !ok || route == nil {
		return true
	}
	if route.APIKeyHeader == "" {
		return true
	}
	return (APIKey{Value: route.APIKeyValue}).matchesValue(req.Header.Get(route.APIKeyHeader))
}
