package runtime

import (
	"context"
	"fmt"
	"net/url"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	dexec "github.com/viant/datly/exec"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/jobs"
)

func (d *jobDispatcher) ReadResult(ctx context.Context, request jobs.ResultRequest) (any, error) {
	record := request.Job
	target, contract, err := d.contract(record)
	if err != nil {
		return nil, err
	}
	registered := d.runtime.registered[target.Component.String()]
	if registered.Handler != nil || registered.Reader == nil || !d.runtime.ExposesComponent(target.Component) {
		return nil, jobs.ErrResultUnavailable
	}
	queries, err := record.QueryScope()
	if err != nil {
		return nil, fmt.Errorf("%w: completed reader query scope is unavailable", jobs.ErrResultUnavailable)
	}
	if settings := registered.Component.Settings; settings != nil && settings.IgnoreEmptyQueryParameters != nil {
		ctx = requestprovider.WithQueryPolicy(ctx, requestprovider.QueryPolicy{IgnoreEmptyParameters: *settings.IgnoreEmptyQueryParameters})
	}
	codec, err := jobs.NewStateCodec(contract)
	if err != nil {
		return nil, err
	}
	var replay *bindly.Replay
	scope := request.Source
	if request.SourceState != "" {
		// This is the just-captured CURRENT request state, never record.State.
		replay, err = codec.Decode(request.SourceState)
	} else {
		if scope == nil {
			return nil, fmt.Errorf("reader results require current request sources")
		}
		uri, parseErr := url.ParseRequestURI(record.URI)
		if parseErr != nil {
			return nil, parseErr
		}
		_, paths, found := d.runtime.bundle.ComponentByRouteWithParams(record.Method, uri.EscapedPath())
		if !found {
			return nil, jobs.ErrResultUnavailable
		}
		// Real target path values come from its durable route instance. Current
		// query/body/credentials remain the native caller providers' authority.
		scope, err = handlerengine.ComposeScope(scope, requestprovider.NewValues(requestprovider.WithPathParams(paths)).Providers()...)
		if err == nil {
			replay, err = codec.CaptureSources(ctx, scope.Providers())
		}
	}
	if err != nil {
		return nil, err
	}
	if _, err = d.prepare(ctx, jobPreparation{record: record, target: target, replay: replay, action: jobs.Inspect}); err != nil {
		return nil, err
	}
	providers, err := replay.Providers()
	if err != nil {
		return nil, err
	}
	public, err := record.Public()
	if err != nil {
		return nil, err
	}
	providers = append(append([]locator.Provider(nil), providers...), &jobs.Presentation{Job: public})
	options := dexec.ReaderOptions{RefreshCache: request.Refresh, QueryScope: queries}
	ctx = (jobs.Invocation{Job: public}).Context(ctx)
	// Dynamic binding data points are freshly evaluated through their real
	// providers; no serialized Current or Previous input is restored. The result
	// graph uses normal native cache lookup and DB fallback, or explicit refresh.
	// Every SQL read still has the durable query guard.
	bindingOptions := dexec.ReaderOptions{RefreshCache: true, QueryScope: queries}
	return d.runtime.invokeComponent(bindingOptions.Context(ctx), dexec.ComponentRequest{Target: target, Providers: providers, Replay: &bindly.ReplayBinding{Replay: replay}, ReaderOptions: &options}, scope)
}
