package transcribe

import (
	"context"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness/sqlite"
	tcolumn "github.com/viant/datly/transcribe/column"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
)

// A previously generated factory holder remains useful type/name authority
// while its authored DQL is converted to a native component lifecycle.
func TestNativeComponentLifecycleDoesNotInheritFactory(t *testing.T) {
	base := &spec.Component{
		Key: spec.Key{Scope: "example.com/events", Name: "Events"}, Name: "Events",
		Routes: []*spec.Route{{Name: "Events", Method: "PATCH", Path: "/events", Handler: "OldFactory"}},
	}
	source := &Source{
		Scope: "example.com/events", Name: "Events", PackageComponent: base,
		LinkedHandler: func() (rhandler.TypedHandler, error) { t.Fatal("stale factory invoked"); return nil, nil },
		Text: `#setting($_ = $route('/events','PATCH'))
#define($_ = $Events<[]*Event>(body/data))
SELECT e.*,type(e,'Event'),lifecycle_type(e,'BatchLifecycle') FROM (events) e`,
	}
	result, err := NewCompiler().Compile(context.Background(), source)
	require.NoError(t, err)
	require.Empty(t, result.Component.Routes[0].Handler, "stale holder execution must not override authored native lifecycle")
	require.Equal(t, "Events", result.Component.Name)
	require.Equal(t, "OldFactory", base.Routes[0].Handler, "source/package authority must not be mutated")
	require.Nil(t, result.Source.LinkedHandler)
	require.NotNil(t, source.LinkedHandler, "compilation must detach its execution selection")
}

func TestNativeLifecycleKeepsOtherHandlerAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, method, directives, graph, handler string
	}{
		{"implicit", "PATCH", "", "SELECT e.* FROM (events) e", "OldFactory"},
		{"physical row hook", "PATCH", "", "SELECT e.*,lifecycle_type(e,'RowHooks') FROM events e", "OldFactory"},
		{"output relation", "PATCH", "#define($_ = $Meta<Meta>(output/summary) /* SELECT COUNT(*) AS count FROM events */)\n", "SELECT e.*,lifecycle_type(e,'BatchLifecycle') FROM (events) e", "OldFactory"},
		{"reader hook", "GET", "", "SELECT e.*,lifecycle_type(e,'RowHooks') FROM (events) e", "OldFactory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := &Source{Scope: "example.com/events", Name: "Events",
				PackageComponent: &spec.Component{Name: "Events", Routes: []*spec.Route{{Method: tc.method, Path: "/events", Handler: "OldFactory"}}},
				LinkedHandler:    func() (rhandler.TypedHandler, error) { return nil, nil },
				Text:             "#setting($_ = $route('/events','" + tc.method + "'))\n" + tc.directives + tc.graph,
			}
			result, err := NewCompiler().Compile(context.Background(), source)
			require.NoError(t, err)
			require.Equal(t, tc.handler, result.Component.Routes[0].Handler)
			require.NotNil(t, result.Source.LinkedHandler)
		})
	}
}

func TestNativeLifecycleKeepsExplicitAuthoredHandler(t *testing.T) {
	source := &Source{Text: "authored", LinkedHandler: func() (rhandler.TypedHandler, error) { return nil, nil }}
	component := &spec.Component{RootView: &spec.View{Auxiliary: true, EntityHooks: "BatchLifecycle"}, Routes: []*spec.Route{{Method: "PATCH", Handler: "ExplicitFactory"}}}
	compilation := &componentCompilation{
		authored: &spec.Component{RootView: &spec.View{}, Routes: []*spec.Route{{Method: "PATCH", Handler: "ExplicitFactory"}}},
		result:   &Result{Component: component, Source: source},
	}
	compilation.selectNativeComponentLifecycle()
	require.Equal(t, "ExplicitFactory", component.Routes[0].Handler)
	require.NotNil(t, source.LinkedHandler)
}

func TestNativeComponentLifecycleEmbeddedSourceDoesNotInheritFactory(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE events(id INTEGER PRIMARY KEY,name TEXT)"))
	resources, err := resource.New().WithDefault(fstest.MapFS{"body.sql": {Data: []byte("SELECT * FROM (events)")}})
	require.NoError(t, err)
	base := &spec.Component{Key: spec.Key{Scope: "example.com/events", Name: "Events"}, Name: "Events", Routes: []*spec.Route{{Method: "PATCH", Path: "/events", Handler: "OldFactory"}}}
	source := &Source{Scope: "example.com/events", Name: "Events", PackageComponent: base, Resources: resources, Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), LinkedHandler: func() (rhandler.TypedHandler, error) { t.Fatal("factory invoked"); return nil, nil }, Text: `#setting($_ = $route('/events','PATCH'))
#define($_ = $Events<[]*Event>(body/data))
SELECT e.*,type(e,'Event'),lifecycle_type(e,'BatchLifecycle') FROM (${embed:body.sql}) e`}
	result, err := NewCompiler().Compile(ctx, source)
	require.NoError(t, err)
	require.True(t, result.Component.RootView.Auxiliary)
	require.Empty(t, result.Component.Routes[0].Handler)
	require.Nil(t, result.Source.LinkedHandler)
	require.NotNil(t, source.LinkedHandler)
	require.Equal(t, "OldFactory", base.Routes[0].Handler)
	require.Equal(t, "Events", result.Component.Name)
}
