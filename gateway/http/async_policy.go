package http

import (
	"context"
	"fmt"
	"github.com/viant/bindly/xform/conv"
	xresponse "github.com/viant/xdatly/response"
	"net/url"
	"reflect"
	"strings"

	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xasync "github.com/viant/xdatly/async"
)

type asyncInputPolicy struct {
	config    AsyncRoute
	input     *registry.RouteInputContract
	mainView  string
	forceSync bool
	owns      func(*xasync.Job) bool
}

func (p *asyncInputPolicy) validate() error {
	fields := map[string]registry.InputField{}
	for _, field := range p.input.Fields() {
		fields[strings.ToLower(field.Path())] = field
		if name := field.Binding().Name; name != "" {
			fields[strings.ToLower(name)] = field
		}
	}
	controls := map[string]reflect.Kind{p.config.MatchKey: reflect.String, p.config.SyncFlag: reflect.Bool}
	if p.config.Inspect != nil {
		controls[p.config.Inspect.JobID] = reflect.String
	}
	for name, kind := range controls {
		if name == "" {
			continue
		}
		field, ok := fields[strings.ToLower(name)]
		if !ok || field.DestinationType().Kind() != kind || !(spec.BindSource{Kind: field.Binding().Location.Kind}).RequestValue() {
			return fmt.Errorf("async control %s requires a canonical external %s field", name, kind)
		}
	}
	if p.config.Inspect == nil && p.config.MatchKey == "" {
		return fmt.Errorf("async match key field is required")
	}
	if p.config.Inspect != nil && (p.config.Inspect.JobID == "" || p.config.MatchKey != "" || p.config.SyncFlag != "" && !p.config.Inspect.Result) {
		return fmt.Errorf("async inspection requires only a job ID control")
	}
	return nil
}
func (p *asyncInputPolicy) Select(_ context.Context, input any) (jobs.Controls, error) {
	result := jobs.Controls{}
	if p.config.Inspect != nil {
		value, found, err := p.value(input, p.config.Inspect.JobID, reflect.TypeFor[string]())
		if err != nil {
			return result, err
		}
		if !found || value.(string) == "" {
			return result, &xresponse.Error{Code: 400, Payload: xresponse.Status{Status: "error", Message: "job ID is required"}}
		}
		result.JobID = value.(string)
		result.Result = p.config.Inspect.Result
		if result.Result && p.config.SyncFlag != "" {
			value, found, err = p.value(input, p.config.SyncFlag, reflect.TypeFor[bool]())
			if err != nil {
				return result, err
			}
			if found {
				result.Sync = value.(bool)
			}
		}
		result.Sync = result.Sync || (result.Result && p.forceSync)
		return result, nil
	}
	value, found, err := p.value(input, p.config.MatchKey, reflect.TypeFor[string]())
	if err != nil {
		return result, err
	}
	if !found || value.(string) == "" {
		return result, &xresponse.Error{Code: 400, Payload: xresponse.Status{Status: "error", Message: "job match key is required"}}
	}
	result.MatchKey = p.mainView + "/" + value.(string)
	if p.config.SyncFlag != "" {
		value, found, err = p.value(input, p.config.SyncFlag, reflect.TypeFor[bool]())
		if err != nil {
			return result, err
		}
		if found {
			result.Sync = value.(bool)
		}
	}
	result.Sync = result.Sync || p.forceSync
	return result, nil
}
func (p *asyncInputPolicy) Owns(job *xasync.Job) bool { return p.owns(job) }
func (h *Handler) ownsAsyncJob(target spec.RouteRef, job *xasync.Job) bool {
	uri, err := url.ParseRequestURI(job.URI)
	if err != nil {
		return false
	}
	route, found := h.runtime.RouteByMethodPath(job.Method, uri.EscapedPath())
	return found && route != nil && (spec.RouteRef{Method: route.Method, Path: route.Path}) == target
}

func (p *asyncInputPolicy) value(input any, name string, target reflect.Type) (any, bool, error) {
	value, found, err := p.input.Resolver(input)(name)
	if err != nil || !found {
		return nil, found, err
	}
	value, err = (conv.ValueConverter{}).Convert(value, target)
	return value, true, err
}
