package transcribe

import (
	"context"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness/sqlite"
	tcolumn "github.com/viant/datly/transcribe/column"
	"reflect"
	"strings"
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

func TestNativeAuxiliaryGraphExecutionAuthority(t *testing.T) {
	useCases := []struct {
		name  string
		edit  func(*componentCompilation)
		clear bool
	}{
		{name: "nested local and package-linked auxiliary body", clear: true},
		{name: "existing in-memory leaf selection unchanged", clear: true, edit: func(c *componentCompilation) {
			root := c.result.Component.RootView
			root.Relations = nil
			root.InMemory = true
		}},
		{name: "explicit authored factory", edit: func(c *componentCompilation) { c.authored.Routes[0].Handler = "ExplicitFactory" }},
		{name: "physical descendant", edit: func(c *componentCompilation) { c.result.Component.RootView.Relations[0].View.Auxiliary = false }},
		{name: "derived output relation", edit: func(c *componentCompilation) {
			c.result.Component.RootView.Relations[0].Kind = spec.RelationKindDerived
		}},
		{name: "in-memory descendant", edit: func(c *componentCompilation) { c.result.Component.RootView.Relations[0].View.InMemory = true }},
		{name: "self reference", edit: func(c *componentCompilation) {
			c.result.Component.RootView.Relations[0].View.SelfReference = &spec.SelfReference{}
		}},
		{name: "incomplete relation", edit: func(c *componentCompilation) { c.result.Component.RootView.Relations[0].View = nil }},
		{name: "cyclic body", edit: func(c *componentCompilation) {
			root := c.result.Component.RootView
			root.Relations[0].View.Relations = []*spec.Relation{{View: root}}
		}},
		{name: "no lifecycle", edit: func(c *componentCompilation) { c.result.Component.RootView.EntityHooks = "" }},
		{name: "reader route", edit: func(c *componentCompilation) { c.authored.Routes[0].Method = "GET" }},
		{name: "no authored source", edit: func(c *componentCompilation) { c.result.Source.Text = "" }},
	}
	for _, useCase := range useCases {
		t.Run(useCase.name, func(t *testing.T) {
			original := &Source{Text: "authored DQL", LinkedHandler: func() (rhandler.TypedHandler, error) { t.Fatal("factory invoked"); return nil, nil }, LinkedInputType: reflect.TypeFor[spec.Component](), LinkedOutputType: reflect.TypeFor[spec.View]()}
			detached := *original
			base := &spec.Component{Name: "Events", Routes: []*spec.Route{{Method: "PATCH", Handler: "OldFactory"}}}
			compiled := base.Clone()
			compiled.RootView = &spec.View{Auxiliary: true, EntityHooks: "BatchLifecycle", Relations: []*spec.Relation{
				{Holder: "Children", View: &spec.View{Auxiliary: true, Relations: []*spec.Relation{{Holder: "Nested", View: &spec.View{Auxiliary: true}}}}},
				{Holder: "Flights", View: &spec.View{Auxiliary: true, TypeName: "example.com/flight.Flight"}},
			}}
			compilation := &componentCompilation{authored: &spec.Component{RootView: &spec.View{}, Routes: []*spec.Route{{Method: "PATCH"}}}, result: &Result{Source: &detached, Component: compiled}}
			if useCase.edit != nil {
				useCase.edit(compilation)
			}
			compilation.selectNativeComponentLifecycle()
			if useCase.clear {
				require.Empty(t, compiled.Routes[0].Handler)
				require.Nil(t, detached.LinkedHandler)
			} else {
				require.Equal(t, "OldFactory", compiled.Routes[0].Handler)
				require.NotNil(t, detached.LinkedHandler)
			}
			require.Equal(t, "OldFactory", base.Routes[0].Handler)
			require.NotNil(t, original.LinkedHandler)
			require.Equal(t, original.LinkedInputType, detached.LinkedInputType)
			require.Equal(t, original.LinkedOutputType, detached.LinkedOutputType)
		})
	}
}

func TestNativeAuxiliaryGraphEmbeddedSourceClearsInheritedFactory(t *testing.T) {
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE events(id INTEGER PRIMARY KEY)", "CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER)", "CREATE TABLE nested(id INTEGER PRIMARY KEY,parent_id INTEGER)"))
	resources, err := resource.New().WithDefault(fstest.MapFS{
		"events.sql":   {Data: []byte("SELECT * FROM (events)")},
		"children.sql": {Data: []byte("SELECT * FROM (children)")},
		"nested.sql":   {Data: []byte("SELECT * FROM (nested)")},
	})
	require.NoError(t, err)
	base := &spec.Component{Name: "Events", Routes: []*spec.Route{{Method: "PATCH", Path: "/events", Handler: "OldFactory"}}}
	source := &Source{Scope: "example.com/events", Name: "Events", PackageComponent: base, Resources: resources, Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), LinkedHandler: func() (rhandler.TypedHandler, error) { t.Fatal("factory invoked"); return nil, nil }, Text: `#setting($_ = $route('/events','PATCH'))
#define($_ = $Events<[]*Event>(body/data))
SELECT e.*,type(e,'Event'),lifecycle_type(e,'BatchLifecycle'),
 c.*,type(c,'Child','Children'),n.*,type(n,'Nested','Nested')
FROM (${embed:events.sql}) e
LEFT JOIN (${embed:children.sql}) c ON c.parent_id=e.id
LEFT JOIN (${embed:nested.sql}) n ON n.parent_id=c.id`}
	result, err := NewCompiler().Compile(t.Context(), source)
	require.NoError(t, err)
	require.True(t, result.Component.RootView.Auxiliary)
	require.True(t, result.Component.RootView.Relations[0].View.Auxiliary)
	require.True(t, result.Component.RootView.Relations[0].View.Relations[0].View.Auxiliary)
	require.Empty(t, result.Component.Routes[0].Handler)
	require.Nil(t, result.Source.LinkedHandler)
	require.NotNil(t, source.LinkedHandler)
	require.Equal(t, "OldFactory", base.Routes[0].Handler)
}

func TestNativeAuxiliaryGraphRejectsInvalidHookDespiteInheritedFactory(t *testing.T) {
	text := strings.Replace(auxiliaryOwnerDQL, `tag(r.caller,'sqlx:"-" json:"-"')`, `tag(r.caller,'sqlx:"-" json:"-"'),c.*,type(c,'Child','Children'),CAST(c.id AS int),CAST(c.type_id AS int)`, 1)
	text += "\nLEFT JOIN (SELECT id,type_id FROM (records)) c ON c.type_id=r.id"
	root, source := auxiliaryOwnerFixture(t, text)
	source.PackageComponent = &spec.Component{Routes: []*spec.Route{{Method: "PATCH", Path: "/records", Handler: "OldFactory"}}}
	source.LinkedHandler = func() (rhandler.TypedHandler, error) { t.Fatal("factory invoked"); return nil, nil }
	writeSourceFile(t, root, "generated/lifecycle.go", `package generated
import("context";h "github.com/viant/xdatly/handler")
type BatchLifecycle struct{}
func(*BatchLifecycle)Init(context.Context,*Record,h.LifecycleContext[Record,h.NoParent,Output])error{return nil}
func(*BatchLifecycle)Validate(context.Context,*Record,h.LifecycleContext[Record,h.NoParent,Output])error{return nil}
`)
	_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
	require.ErrorContains(t, err, "incompatible signature")
	require.Equal(t, "OldFactory", source.PackageComponent.Routes[0].Handler)
	require.NotNil(t, source.LinkedHandler)
}
