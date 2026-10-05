package application_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	xasync "github.com/viant/xdatly/async"
	xexec "github.com/viant/xdatly/exec"
)

type routingAsyncInput struct {
	ID    string   `parameter:"ID,kind=path,in=id"`
	Key   string   `parameter:"Key,kind=query,in=key"`
	Query []string `parameter:"Query,kind=query,in=q"`
}
type routingAsyncStatusInput struct {
	JobID string `parameter:"JobID,kind=path,in=jobid"`
}

type routingAsyncOutput struct {
	Job    *xasync.Job `parameter:"kind=async,in=job" json:"job,omitempty"`
	Code   string      `parameter:"kind=async,in=jobinfo.code" json:"code"`
	Status string      `parameter:"kind=output,in=status" json:"status"`
	ID     string      `json:"id"`
	Query  []string    `json:"query"`
	Route  string      `json:"route"`
	URI    string      `json:"uri"`
}

func TestRoutingAsyncDurableCanonicalReplaySQLite(t *testing.T) {
	for _, mode := range []string{"", "escaped", "decoded"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			f := &asyncAppFixture{}
			f.init(t, false)
			compile := func(_ context.Context, _ *typecatalog.Catalog) (*application.Build, error) {
				var components []*registry.RegisteredComponent
				var routes []gateway.AsyncRoute
				for i, path := range []string{"/jobs/{id}", "/jobs/a/{id}"} {
					label := []string{"escaped", "decoded"}[i]
					component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Routing" + label}, Name: "Routing" + label, Routes: []*spec.Route{{Method: "GET", Path: path}}}
					a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[routingAsyncInput](), OutputType: reflect.TypeFor[routingAsyncOutput]()})
					if err != nil {
						return nil, err
					}
					handler := rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
						in := inv.Input.(*routingAsyncInput)
						execution := xexec.GetContext(ctx)
						if execution == nil {
							return nil, fmt.Errorf("replay execution context absent")
						}
						return &routingAsyncOutput{ID: in.ID, Query: in.Query, Route: label, URI: execution.URI}, nil
					})
					components = append(components, &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[routingAsyncOutput](), Handler: handler})
					routes = append(routes, gateway.AsyncRoute{Route: spec.RouteRef{Method: "GET", Path: path}, MatchKey: "Key"})
					statusPath := "/routing-status/" + label + "/{jobid}"
					statusComponent := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "RoutingStatus" + label}, Routes: []*spec.Route{{Method: "GET", Path: statusPath}}}
					status, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: statusComponent, InputType: reflect.TypeFor[routingAsyncStatusInput](), OutputType: reflect.TypeFor[routingAsyncOutput]()})
					if err != nil {
						return nil, err
					}
					components = append(components, &registry.RegisteredComponent{Component: status.Component, Input: status.Input, Output: status.Output, OutputType: reflect.TypeFor[routingAsyncOutput](), Handler: rhandler.HandlerFunc(func(context.Context, rhandler.Invocation) (any, error) {
						return nil, fmt.Errorf("status handler must not execute")
					})})
					routes = append(routes, gateway.AsyncRoute{Route: spec.RouteRef{Method: "GET", Path: statusPath}, Inspect: &gateway.AsyncInspect{JobID: "JobID", Target: spec.RouteRef{Method: "GET", Path: path}}})

				}
				return &application.Build{Components: components, HTTP: gateway.Config{PathSemantics: mode, Async: routes}}, nil
			}
			manager, err := application.New(nil, application.WithAsync(f.config))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, manager.Shutdown(ctx)) })
			require.NoError(t, manager.Reload(ctx, application.Request{Revision: 1, Compile: compile}))
			for index, encoded := range []string{"a%2Fb", "a%252Fb"} {
				original := "/jobs/" + encoded + fmt.Sprintf("?q=first%%2Fsecond&q=last&key=case%d", index)
				req := httptest.NewRequest("GET", original, nil)
				before, uri := *req.URL, req.RequestURI
				response := httptest.NewRecorder()
				manager.ServeHTTP(response, req)
				require.Equal(t, 200, response.Code, response.Body.String())
				require.Equal(t, before, *req.URL)
				require.Equal(t, uri, req.RequestURI)
				var admitted routingAsyncOutput
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &admitted))
				require.Equal(t, "WAITING", admitted.Code)
				require.NotNil(t, admitted.Job)
				canonical := original
				id, label := "a/b", "escaped"
				if index == 1 {
					id = "a%2Fb"
				}
				if mode == "decoded" && index == 0 {
					canonical = "/jobs/a/b" + original[len("/jobs/a%2Fb"):]
					id, label = "b", "decoded"
				}
				durable, err := f.store.Get(ctx, admitted.Job.ID)
				require.NoError(t, err)
				require.Equal(t, canonical, durable.URI)
				require.Equal(t, xasync.StatusPending, durable.Status)
				// Reload before execution: persisted target/state authority must survive a new host generation.
				require.NoError(t, manager.Reload(ctx, application.Request{Revision: uint64(index + 2), Compile: compile}))
				actual, err := manager.HandleJob(ctx, &jobs.Event{Job: *admitted.Job})
				require.NoError(t, err)
				out, ok := actual.(*routingAsyncOutput)
				require.True(t, ok, "%T", actual)
				require.Equal(t, id, out.ID)
				require.Equal(t, label, out.Route)
				require.Equal(t, []string{"first/second", "last"}, out.Query)
				require.Equal(t, canonical, out.URI)
				completed, err := f.store.Get(ctx, admitted.Job.ID)
				require.NoError(t, err)
				require.Equal(t, canonical, completed.URI)
				require.Equal(t, xasync.StatusDone, completed.Status)
				status, err := manager.JobStatus(ctx, admitted.Job.ID)
				require.NoError(t, err)
				require.Equal(t, canonical, status.URI)
				require.Equal(t, xasync.StatusDone, status.Status)
				statusRequest := httptest.NewRequest("GET", "/routing-status/"+label+"/"+admitted.Job.ID, nil)
				statusResponse := httptest.NewRecorder()
				manager.ServeHTTP(statusResponse, statusRequest)
				require.Equal(t, 200, statusResponse.Code, statusResponse.Body.String())
				var statusOutput routingAsyncOutput
				require.NoError(t, json.Unmarshal(statusResponse.Body.Bytes(), &statusOutput))
				require.Equal(t, "COMPLETE", statusOutput.Code)
				require.Equal(t, canonical, statusOutput.Job.URI)
				other := "decoded"
				if label == "decoded" {
					other = "escaped"
				}
				wrongRequest := httptest.NewRequest("GET", "/routing-status/"+other+"/"+admitted.Job.ID, nil)
				wrongResponse := httptest.NewRecorder()
				manager.ServeHTTP(wrongResponse, wrongRequest)
				require.Equal(t, 404, wrongResponse.Code, wrongResponse.Body.String())

			}
			// A direct job keeps native escaped semantics even when HTTP host policy is decoded.
			directURI := "/jobs/a%2Fb?q=direct%2Fvalue&key=direct"
			direct, err := manager.ScheduleJob(ctx, jobs.Submission{Job: xasync.Job{Request: xasync.Request{Method: "GET", URI: directURI}}, SourceState: `{"ID":"a/b","Key":"direct","Query":["direct/value"]}`})
			require.NoError(t, err)
			stored, err := f.store.Get(ctx, direct.Job.ID)
			require.NoError(t, err)
			require.Equal(t, directURI, stored.URI)
			actual, err := manager.HandleJob(ctx, &jobs.Event{Job: *direct.Job})
			require.NoError(t, err)
			out := actual.(*routingAsyncOutput)
			require.Equal(t, "escaped", out.Route)
			require.Equal(t, "a/b", out.ID)
			require.Equal(t, []string{"direct/value"}, out.Query)
			require.Equal(t, directURI, out.URI)
		})
	}
}
