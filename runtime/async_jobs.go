package runtime

import (
	"context"
	"fmt"
	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	xexec "github.com/viant/xdatly/exec"
	xhandler "github.com/viant/xdatly/handler"
	"net/http"
	"net/url"
)

// NewAsyncService wires durable jobs over canonical component execution.
// Transport publication and watcher lifetime are configured explicitly.
func (r *Runtime) NewAsyncService(config jobs.Config) (*jobs.Service, error) {
	if r == nil || r.bundle == nil || r.invoker == nil {
		return nil, fmt.Errorf("async runtime is not initialized")
	}
	return jobs.NewService(config, &jobDispatcher{runtime: r, authorize: config.Authorize})
}

type jobDispatcher struct {
	runtime   *Runtime
	authorize jobs.Authorizer
}
type jobExecution struct {
	runtime *Runtime
	request dexec.ComponentRequest
	method  string
	uri     string
}

func (e *jobExecution) Execute(ctx context.Context) (*jobs.ExecutionResult, error) {
	result := &jobs.ExecutionResult{}
	executionContext := xexec.New(xexec.WithMethod(e.method), xexec.WithURI(e.uri))
	ctx = xexec.WithContext(ctx, executionContext)
	request := e.request
	if invocation, ok := jobs.CurrentInvocation(ctx); ok {
		request.Providers = append(append([]locator.Provider(nil), request.Providers...), &jobs.Presentation{Job: invocation.Job})
	}
	request.Completion = func(outcome xhandler.Outcome) { copy := outcome.Clone(); result.Outcome = &copy }
	uri, err := url.ParseRequestURI(e.uri)
	if err != nil {
		return result, err
	}
	synthetic, err := http.NewRequestWithContext(ctx, e.method, (&url.URL{Scheme: "http", Host: "localhost", Path: uri.Path, RawPath: uri.RawPath, RawQuery: uri.RawQuery}).String(), nil)
	if err != nil {
		return result, err
	}
	synthetic.RequestURI = e.uri
	_, pathParams, _ := e.runtime.bundle.ComponentByRouteWithParams(e.method, uri.EscapedPath())
	scope, err := requestprovider.New(synthetic, requestprovider.WithPathParams(pathParams))
	if err != nil {
		return result, err
	}
	defer scope.Close()
	result.Value, err = e.runtime.invokeComponent((dexec.ReaderOptions{RefreshCache: true}).Context(ctx), request, scope)
	result.Metrics = executionContext.SnapshotForLogging().Metrics
	return result, err
}

func (d *jobDispatcher) contract(record *jobs.Record) (dexec.ComponentTarget, *registry.RouteInputContract, error) {
	uri, err := url.ParseRequestURI(record.URI)
	if err != nil {
		return dexec.ComponentTarget{}, nil, err
	}
	component, _, ok := d.runtime.bundle.ComponentByRouteWithParams(record.Method, uri.EscapedPath())
	if !ok || component == nil {
		return dexec.ComponentTarget{}, nil, fmt.Errorf("async registered route not found: %s %s", record.Method, uri.Path)
	}
	route, ok := d.runtime.bundle.RouteByMethodPath(record.Method, uri.EscapedPath())
	if !ok || route == nil {
		return dexec.ComponentTarget{}, nil, fmt.Errorf("async route metadata not found")
	}
	target := dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: route.Method, Path: route.Path}}
	registered, loadErr := d.runtime.registeredComponent(context.Background(), component.Key)
	if loadErr != nil || registered == nil || registered.Input == nil || (registered.Reader == nil && registered.Handler == nil) {
		return target, nil, fmt.Errorf("async registered component execution is unavailable")
	}
	contract, ok := registered.Input.ForRoute(target.Route)
	if !ok {
		return target, nil, fmt.Errorf("async input contract not found")
	}
	return target, contract, nil
}

type jobPreparation struct {
	record *jobs.Record
	target dexec.ComponentTarget
	replay *bindly.Replay
	action jobs.Action
	policy jobs.InputPolicy
}

func (d *jobDispatcher) prepare(ctx context.Context, request jobPreparation) (*jobs.Capture, error) {
	record, target, replay, action, policy := request.record, request.target, request.replay, request.action, request.policy
	providers, err := replay.Providers()
	if err != nil {
		return nil, err
	}
	captured := &jobs.Capture{}
	input, err := d.runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: providers, Replay: &bindly.ReplayBinding{Replay: replay, Only: true, Gate: func(ctx context.Context, input any) error {
		if policy != nil {
			var err error
			captured.Controls, err = policy.Select(ctx, input)
			if err != nil {
				return err
			}
			record.MatchKey = captured.Controls.MatchKey
		}
		if captured.Controls.JobID != "" {
			return nil
		} // inspect authorizes actual durable identity after this codec preflight
		public, err := record.Public()
		if err != nil {
			return err
		}
		if err := d.authorize(ctx, jobs.Access{Action: action, Job: public, Input: input}); err != nil {
			return err
		}
		if policy != nil {
			after, err := policy.Select(ctx, input)
			if err != nil {
				return err
			}
			if after != captured.Controls {
				return fmt.Errorf("authorization cannot change scheduling controls")
			}
		}
		return nil
	}}})
	captured.Input = input
	return captured, err
}
func (d *jobDispatcher) Capture(ctx context.Context, record *jobs.Record, submission jobs.Submission) (*jobs.Capture, error) {
	target, contract, err := d.contract(record)
	if err != nil {
		return nil, err
	}
	registered, err := d.runtime.registeredComponent(ctx, target.Component)
	if err != nil {
		return nil, err
	}
	if settings := registered.Component.Settings; settings != nil && settings.IgnoreEmptyQueryParameters != nil {
		ctx = requestprovider.WithQueryPolicy(ctx, requestprovider.QueryPolicy{IgnoreEmptyParameters: *settings.IgnoreEmptyQueryParameters})
	}
	codec, err := jobs.NewStateCodec(contract)
	if err != nil {
		return nil, err
	}
	if submission.Input != nil && (submission.SourceState != "" || submission.Source != nil) || submission.Source != nil && submission.SourceState != "" {
		return nil, fmt.Errorf("provide canonical Input or raw SourceState, not both")
	}
	var replay *bindly.Replay
	if submission.Source != nil {
		replay, err = codec.CaptureSources(ctx, submission.Source.Providers())
	} else if submission.SourceState != "" {
		replay, err = codec.Decode(submission.SourceState)
	} else {
		replay, err = codec.Capture(submission.Input)
	}
	if err != nil {
		return nil, err
	}
	captured, err := d.prepare(ctx, jobPreparation{record: record, target: target, replay: replay, action: jobs.Submit, policy: submission.Policy})
	if err != nil {
		return nil, err
	}
	record.State, err = codec.State(replay)
	if err != nil {
		return nil, err
	}
	if submission.Source != nil || captured.Controls.JobID != "" || registered.Handler != nil {
		return captured, nil
	}
	providers, err := replay.Providers()
	if err != nil {
		return nil, err
	}
	planned, err := d.runtime.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: providers, Replay: &bindly.ReplayBinding{Replay: replay}, DryRun: true})
	if err != nil {
		return nil, err
	}
	plan, ok := planned.(*dexec.ReadPlan)
	if !ok {
		return nil, fmt.Errorf("async reader returned invalid plan %T", planned)
	}
	captured.Plan = plan
	return captured, nil
}
func (d *jobDispatcher) Restore(ctx context.Context, record *jobs.Record) (jobs.Execution, error) {
	target, contract, err := d.contract(record)
	if err != nil {
		return nil, err
	}
	codec, err := jobs.NewStateCodec(contract)
	if err != nil {
		return nil, err
	}
	replay, err := codec.Decode(record.State)
	if err != nil {
		return nil, err
	}
	if _, err = d.prepare(ctx, jobPreparation{record: record, target: target, replay: replay, action: jobs.Replay}); err != nil {
		return nil, err
	}
	providers, err := replay.Providers()
	if err != nil {
		return nil, err
	}
	return &jobExecution{runtime: d.runtime, request: dexec.ComponentRequest{Target: target, Providers: providers, Replay: &bindly.ReplayBinding{Replay: replay}}, method: record.Method, uri: record.URI}, nil
}
