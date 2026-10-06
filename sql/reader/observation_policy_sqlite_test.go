package reader_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	xexec "github.com/viant/xdatly/exec"
)

func TestObservationSameNameDistinctNamespacesOneComponentSQLite(t *testing.T) {
	type child struct {
		ID       int `sqlx:"id"`
		ParentID int `sqlx:"parent_id"`
	}
	type parent struct {
		ID       int `sqlx:"id"`
		Children []*child
	}
	ctx := context.Background()
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER)", "CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO parents VALUES(7)", "INSERT INTO children VALUES(11,7)"))
	childView := &spec.View{Name: "records", Namespace: "children", Source: &spec.ViewSource{SQL: "SELECT children.* FROM children WHERE $COLUMN_IN"}}
	rootView := &spec.View{Name: "records", Namespace: "parents", Source: &spec.ViewSource{SQL: "SELECT parents.* FROM parents"}, Relations: []*spec.Relation{{Name: "children", Holder: "Children", Cardinality: spec.CardinalityMany, View: childView, On: []*spec.RelationLink{{ParentNamespace: "parents", ParentColumn: "id", ChildNamespace: "children", ChildColumn: "parent_id"}}}}}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "module.namespaces", Name: "Records"}, RootView: rootView}
	typ := reflect.TypeFor[[]*parent]()
	plan, err := compiler.Compile(compiler.Input{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, DirectViewType: typ})
	require.NoError(t, err)
	e, err := reader.NewExecution(reader.Config{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: typ, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
	require.NoError(t, err)
	before, err := json.Marshal(component)
	require.NoError(t, err)
	p := &observability.Policy{}
	identities := map[string]bool{}
	for _, target := range e.ObservationTargets() {
		require.Equal(t, "records", target.View.Name)
		identity, err := target.View.Identity()
		require.NoError(t, err)
		identities[identity] = true
		p.Views = append(p.Views, observability.ViewObservation{Component: component.Key, ViewIdentity: identity, Diagnostic: target.View.Namespace + "#", Operation: &observability.OperationDescriptor{Name: "platform.namespace." + target.View.Namespace, Location: "platform/namespace", Description: target.View.Namespace + " performance", Provider: observability.Source11}})
	}
	require.Len(t, identities, 2, "same logical names have two existing effective namespace identities")
	owner := observability.NewRecorder(nil, observability.WithPolicy(p))
	require.NoError(t, owner.ValidateTargets(e.ObservationTargets(), component.Key))
	ec := xexec.New()
	value, err := e.WithRecorder(owner).Read(xexec.WithContext(ctx, ec), &struct{}{}, nil, nil)
	require.NoError(t, err)
	rows := value.([]*parent)
	require.Len(t, rows, 1)
	require.Equal(t, 7, rows[0].ID)
	require.Len(t, rows[0].Children, 1)
	require.Equal(t, 11, rows[0].Children[0].ID)
	require.Equal(t, 7, rows[0].Children[0].ParentID)
	require.Len(t, ec.Metrics, 2)
	for _, metric := range ec.Metrics {
		require.Contains(t, []string{"parents#", "children#"}, metric.View)
		require.Len(t, metric.Executions, 1)
		require.Contains(t, metric.Executions[0].SQL, strings.TrimSuffix(metric.View, "#"))
	}
	for _, namespace := range []string{"parents", "children"} {
		values := owner.Values("platform.namespace." + namespace)
		require.Len(t, values, 11)
		require.EqualValues(t, 1, values["Success"])
		require.Zero(t, values["Error"])
		require.Zero(t, values["Pending"])
		require.EqualValues(t, 1, owner.Cumulative("platform.namespace."+namespace, "count"))
	}
	require.Empty(t, owner.Values(component.Key.String()+"/records"))
	after, err := json.Marshal(component)
	require.NoError(t, err)
	require.Equal(t, before, after, "namespace, SQL, links, and execution metadata stay unchanged")
}

func TestObservationTwoModulesShareRealOwnersSQLite(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO records VALUES(1)", "INSERT INTO children VALUES(10,1)"))
	var registrations []*runtime.RegisteredComponent
	var artifacts []*bootstrap.Artifact
	policy := &observability.Policy{}
	for i, module := range []string{"module.one", "module.two", "module.unrelated"} {
		component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: module, Name: "Observed"}, Name: "Observed", Routes: []*spec.Route{{Method: "GET", Path: fmt.Sprintf("/observed/%d", i)}}}
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[observationRelations]()})
		require.NoError(t, err)
		execution, err := reader.NewExecution(reader.Config{Component: artifact.Component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[observationRelations](), Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}})
		require.NoError(t, err)
		if i < 2 {
			for _, target := range execution.ObservationTargets() {
				identity, err := target.View.Identity()
				require.NoError(t, err)
				policy.Views = append(policy.Views, observability.ViewObservation{Component: component.Key, ViewIdentity: identity, Diagnostic: fmt.Sprintf("%s#%d", target.View.Name, i), Operation: &observability.OperationDescriptor{Name: "platform.delivery.advertiser." + target.View.Name, Location: "platform/delivery/advertiser", Description: target.View.Name + " performance", Provider: observability.Source11}})
			}
		}
		artifacts = append(artifacts, artifact)
		registrations = append(registrations, &runtime.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[observationRelations](), Reader: execution})
	}
	before, err := json.Marshal([]*spec.Component{artifacts[0].Component, artifacts[1].Component, artifacts[2].Component})
	require.NoError(t, err)
	app, err := runtime.NewRuntime(registrations, runtime.WithObservability(runtime.ObservabilityConfig{Policy: policy}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, app.Shutdown(ctx)) })
	for i, artifact := range artifacts {
		ec := xexec.New()
		actual, err := app.InvokeComponent(xexec.WithContext(ctx, ec), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: fmt.Sprintf("/observed/%d", i)}}, Input: &struct{}{}})
		require.NoError(t, err)
		require.Equal(t, 10, actual.(*observationRelations).Rows[0].Children[0].ID)
		require.Len(t, ec.Metrics, 2)
		for _, metric := range ec.Metrics {
			if i < 2 {
				require.Contains(t, []string{fmt.Sprintf("records#%d", i), fmt.Sprintf("children#%d", i)}, metric.View)
			} else {
				require.Contains(t, []string{"records", "children"}, metric.View)
			}
		}
	}
	recorder := app.Observability().Recorder
	for _, name := range []string{"records", "children"} {
		values := recorder.Values("platform.delivery.advertiser." + name)
		require.Len(t, values, 11)
		require.EqualValues(t, 2, values["Success"])
		require.Zero(t, values["Pending"])
		for i := 0; i < 2; i++ {
			require.Empty(t, recorder.Values(artifacts[i].Component.Key.String()+"/"+name), "source owner must capture directly, without duplicate native counts")
		}
		native := recorder.Values(artifacts[2].Component.Key.String() + "/" + name)
		require.Len(t, native, 14)
		require.EqualValues(t, 1, native["Success"])
		require.Zero(t, native["Pending"])
	}
	after, err := json.Marshal([]*spec.Component{artifacts[0].Component, artifacts[1].Component, artifacts[2].Component})
	require.NoError(t, err)
	require.Equal(t, before, after, "observation must not change execution metadata")
	// Child preparation fails after root SELECT has already succeeded. That
	// original lifecycle charges one child error, without retroactive root error.
	require.NoError(t, db.ExecStatements(ctx, "DROP TABLE children"))
	ec := xexec.New()
	_, err = app.InvokeComponent(xexec.WithContext(ctx, ec), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifacts[0].Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/observed/0"}}, Input: &struct{}{}})
	require.Error(t, err)
	root := recorder.Values("platform.delivery.advertiser.records")
	child := recorder.Values("platform.delivery.advertiser.children")
	require.EqualValues(t, 3, root["Success"])
	require.Zero(t, root["Error"])
	require.Zero(t, root["Pending"])
	require.EqualValues(t, 2, child["Success"])
	require.EqualValues(t, 1, child["Error"])
	require.Zero(t, child["Pending"])
}
