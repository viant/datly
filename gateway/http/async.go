package http

import (
	"context"
	"errors"
	"fmt"
	"github.com/viant/bindly"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/output"
	"github.com/viant/datly/spec"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	xasync "github.com/viant/xdatly/async"
	xresponse "github.com/viant/xdatly/response"
	"net/http"
	"reflect"
)

func (h *Handler) asyncRoute(req *http.Request) *asyncRoute {
	if h.async == nil {
		return nil
	}
	route, ok := h.runtime.RouteByMethodPath(req.Method, req.URL.EscapedPath())
	if !ok || route == nil {
		return nil
	}
	return h.async.routes[(spec.RouteRef{Method: route.Method, Path: route.Path}).String()]
}
func (r *asyncRoute) execute(ctx context.Context, req *http.Request, scope exec.ProviderScope, service AsyncService) (any, error) {
	// The request path is the matched route instance, not a client-supplied target.
	// Raw query/source values live in canonical SourceState instead of URI replay.
	if r.targetAPIKeyHeader != "" && !(APIKey{Value: r.targetAPIKeyValue}).matchesValue(req.Header.Get(r.targetAPIKeyHeader)) {
		return nil, &xresponse.Error{Code: 403, Payload: xresponse.Status{Status: "error", Message: "forbidden"}}
	}
	uri := req.URL.EscapedPath()
	result, err := service.Exchange(ctx, jobs.Submission{Job: xasync.Job{Request: xasync.Request{Method: req.Method, URI: uri}, MainView: r.mainView, Module: r.module, JobType: r.jobType}, Source: scope, Policy: r.policy})
	if errors.Is(err, cache.ErrMiss) {
		return nil, &xresponse.Error{Code: 410, Payload: xresponse.Status{Status: "error", Message: "completed reader cache entry is missing or expired"}}
	}
	if errors.Is(err, sqlxread.ErrQueryOutsideScope) {
		return nil, &xresponse.Error{Code: 409, Payload: xresponse.Status{Status: "error", Message: "current reader query is incompatible with the completed job"}}
	}
	if errors.Is(err, cache.ErrLookupUnsupported) {
		return nil, &xresponse.Error{Code: 503, Payload: xresponse.Status{Status: "error", Message: cache.ErrLookupUnsupported.Error()}}
	}
	if errors.Is(err, jobs.ErrNotFound) {
		return nil, &xresponse.Error{Code: 404, Payload: xresponse.Status{Status: "error", Message: "job not found"}}
	}
	if errors.Is(err, jobs.ErrResultUnavailable) {
		return nil, &xresponse.Error{Code: 409, Payload: xresponse.Status{Status: "error", Message: jobs.ErrResultUnavailable.Error()}}
	}
	if errors.Is(err, jobs.ErrInProgress) {
		return nil, &xresponse.Error{Code: 409, Payload: xresponse.Status{Status: "error", Message: "job is already running"}}
	}
	if result == nil || result.Job == nil {
		return nil, err
	}
	value := result.Value
	if value == nil {
		value = reflect.New(r.outputType).Interface()
	}
	if reflect.TypeOf(value) != reflect.PointerTo(r.outputType) {
		return nil, fmt.Errorf("async result does not match registered output type")
	}
	bound, bindErr := r.injector.ForScope(&jobs.Presentation{Job: result.Job, Error: err}, &jobs.StatusPresentation{Error: err})
	if bindErr == nil {
		bindErr = bound.Bind(ctx, value, bindly.WithPlan(r.output))
	}
	return value, errors.Join(err, bindErr)
}

func (h *Handler) prepareAsyncRoute(req *http.Request, route *asyncRoute) (*asyncRoute, error) {
	format := h.outputFormat(req)
	if _, err := output.ContentType(format); err != nil {
		return nil, &xresponse.Error{Code: 400, Payload: xresponse.Status{Status: "error", Message: err.Error()}}
	}
	if format == "xls" {
		copy := *route
		policy := *route.policy
		policy.forceSync = true
		copy.policy = &policy
		return &copy, nil
	}
	return route, nil
}
