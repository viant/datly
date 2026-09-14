package application_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/application"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/typecatalog"
	xasync "github.com/viant/xdatly/async"
)

func TestHTTPAsyncExactStatusOwnerAndVisibilitySQLite(t *testing.T) {
	f := &httpAsyncFixture{}
	f.init(t, false)
	manager, err := application.New(nil, application.WithAsync(f.config))
	require.NoError(t, err)
	f.manager = manager
	defer manager.Shutdown(context.Background())
	compile := func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		built, err := f.compile(ctx, types)
		if err != nil {
			return nil, err
		}
		for _, component := range built.Components {
			component.Component.Key.Scope = "example.com/public"
			if component.Component.Name == "Current" {
				component.Component.Key.Scope = "example.com/private"
			}
		}
		built.RuntimeOptions = append(built.RuntimeOptions, druntime.WithExposedPackages([]string{"example.com/public"}, nil))
		return built, nil
	}
	require.NoError(t, manager.Reload(context.Background(), application.Request{Revision: 1, Compile: compile}))
	hidden := f.request("GET", "/current?id=7", "", "allowed")
	require.Equal(t, 404, hidden.Code)
	response := f.request("PATCH", "/inventory?id=7&key=target&target=/current", `{"quantity":0}`, "allowed")
	require.Equal(t, 200, response.Code, response.Body.String())
	var result httpAsyncOutput
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Equal(t, "PATCH", result.Job.Method)
	require.Equal(t, "/inventory", result.Job.URI)
	other, err := manager.ScheduleJob(context.Background(), jobs.Submission{Job: xasync.Job{Request: xasync.Request{Method: "GET", URI: "/current"}}, SourceState: `{"ID":7}`})
	require.NoError(t, err)
	inspected := f.request("GET", "/job-status/"+other.Job.ID, "", "allowed")
	require.Equal(t, 404, inspected.Code, inspected.Body.String())
	denied := f.request("GET", "/job-status/"+result.Job.ID, "", "wrong")
	require.Equal(t, 403, denied.Code)
	// Making a configured async endpoint private invalidates the stage before
	// publication, preserving the current authorized public snapshot.
	invalid := func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		built, err := compile(ctx, types)
		if err != nil {
			return nil, err
		}
		built.RuntimeOptions = []druntime.Option{druntime.WithExposedPackages([]string{"example.com/private"}, nil)}
		return built, nil
	}
	require.Error(t, manager.Reload(context.Background(), application.Request{Revision: 2, Compile: invalid}))
	require.EqualValues(t, 1, manager.Revision())
}
