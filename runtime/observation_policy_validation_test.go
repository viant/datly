package runtime

import (
	"context"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

type observationValidationRow struct {
	ID int `sqlx:"id"`
}
type observationValidationOutput struct{ Rows []*observationValidationRow }

func TestObservationLazyNamespaceValidationBeforeSQLSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	// No records table exists: invalid policy resolution must fail before SQL.
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "module.namespace", Name: "Records"}, Name: "Records", Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, RootView: &spec.View{Name: "records", Namespace: "actual", Source: &spec.ViewSource{SQL: "SELECT id FROM records"}}, Parameters: []*spec.Parameter{{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
	a, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[observationValidationOutput](), DirectViewField: "Rows"})
	require.NoError(t, err)
	e, err := a.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: h.DB}})
	require.NoError(t, err)
	registered := &registry.RegisteredComponent{Component: a.Component, Input: a.Input, Output: a.Output, OutputType: reflect.TypeFor[observationValidationOutput](), Reader: e}
	wanted, err := (&spec.View{Name: "records", Namespace: "expected"}).Identity()
	require.NoError(t, err)
	p := &observability.Policy{Views: []observability.ViewObservation{{Component: component.Key, ViewIdentity: wanted, Diagnostic: "records#", Operation: &observability.OperationDescriptor{Name: "platform.records", Location: "platform", Description: "records performance", Provider: observability.Source11}}}}
	owner, err := NewObservability(ObservabilityConfig{Policy: p})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, owner.Shutdown(ctx)) })
	options := []Option{WithManagedObservability(owner)}
	lazy, err := NewIndexedRuntime([]*spec.Component{a.Component}, nil, remoteFamilyLoader{component: registered}, options...)
	require.NoError(t, err, "indexed metadata validation does not eagerly materialize views")
	t.Cleanup(func() { require.NoError(t, lazy.Shutdown(ctx)) })
	_, err = lazy.LoadComponent(ctx, component.Key)
	require.ErrorContains(t, err, "observation view target not found")
	require.NotContains(t, err.Error(), "no such table")
	require.Empty(t, owner.Recorder.Values("platform.records"))
	w := httptest.NewRecorder()
	owner.Recorder.ServeMetrics("/metric/", w, httptest.NewRequest("GET", "/metric/operations", nil))
	require.JSONEq(t, "null", w.Body.String())
	_, err = NewRuntime([]*RegisteredComponent{registered}, options...)
	require.ErrorContains(t, err, "observation view target not found", "eager and lazy plans enforce the same namespace identity")
	unknown := component.Clone()
	unknown.Key.Scope = "different.module"
	_, err = NewIndexedRuntime([]*spec.Component{unknown}, nil, remoteFamilyLoader{component: registered}, options...)
	require.ErrorContains(t, err, "observation component target not found")
	require.Empty(t, owner.Recorder.Values("platform.records"))
}
