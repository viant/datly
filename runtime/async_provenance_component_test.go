package runtime

import (
	"context"
	"errors"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	xasync "github.com/viant/xdatly/async"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"testing"
)

type replayDependencyRow struct {
	ID      int
	Version string
}
type replayDependencyOutput struct{ Data []*replayDependencyRow }
type replayDependencyInput struct{ ID int }
type replayParentInput struct {
	ID      int
	Current *replayDependencyOutput
	Has     struct{ ID, Current bool } `setMarker:"true"`
}

func TestAsyncRefreshesComponentDependencyWithScopedExternalValuesSQLite(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, sqlite.DatlyJobsSchema, "CREATE TABLE versions(ID INTEGER, VERSION TEXT)", "INSERT INTO versions VALUES(7,'before'),(999,'wrong-fallback')"); err != nil {
		t.Fatal(err)
	}
	child := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Version"}, Routes: []*spec.Route{{Method: "GET", Path: "/version"}}, Parameters: []*spec.Parameter{{Name: "ID", Source: spec.BindSource{Kind: "query", Name: "id"}}, {Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}, RootView: &spec.View{Name: "Version", Source: &spec.ViewSource{SQL: "SELECT ID,VERSION FROM versions WHERE ID=:ID"}}}
	childArtifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: child, InputType: reflect.TypeOf(replayDependencyInput{}), OutputType: reflect.TypeOf(replayDependencyOutput{}), DirectViewField: "Data"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := childArtifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	if err != nil {
		t.Fatal(err)
	}
	parent := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Parent"}, Routes: []*spec.Route{{Method: "POST", Path: "/parent"}}, Parameters: []*spec.Parameter{{Name: "Current", Source: spec.BindSource{Kind: "component", Name: "GET:/version"}}, {Name: "ID", Source: spec.BindSource{Kind: "query", Name: "id"}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: parent, InputType: reflect.TypeOf(replayParentInput{}), OutputType: reflect.TypeOf(replayDependencyOutput{})})
	if err != nil {
		t.Fatal(err)
	}
	handler := rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
		input := inv.Input.(*replayParentInput)
		if !input.Has.ID || input.ID != 7 || input.Current == nil || len(input.Current.Data) != 1 || input.Current.Data[0].Version != "fresh" {
			return nil, errors.New("component dependency did not bind fresh/scoped data")
		}
		if _, found, err := inv.Binder.Lookup(ctx, xhandler.ValueKey("jwt")); err != nil || found {
			return nil, errors.New("undeclared JWT became ambient principal")
		}
		return input.Current, nil
	})
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(replayDependencyOutput{}), Handler: handler}, {Component: childArtifact.Component, Input: childArtifact.Input, OutputType: reflect.TypeOf(replayDependencyOutput{}), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	store, err := (bootstrap.JobStoreConfig{SQL: &dsql.SQLComponent{DB: db.DB}}).NewStore(ctx)
	if err != nil {
		t.Fatal(err)
	}
	service, err := rt.NewAsyncService(jobs.Config{Store: store, Authorize: func(_ context.Context, access jobs.Access) error {
		if access.Input != nil && access.Input.(*replayParentInput).Current != nil {
			t.Fatal("dependency replayed during authorization")
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	scheduled, err := service.Schedule(ctx, jobs.Submission{Job: xasync.Job{Request: xasync.Request{Method: "POST", URI: "/parent?id=999"}}, SourceState: `{"ID":7,"Current":{"Data":[{"ID":999,"Version":"stale"}]},"JWT":{"sub":"forged"}}`})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.ExecStatements(ctx, "UPDATE versions SET VERSION='fresh' WHERE ID=7"); err != nil {
		t.Fatal(err)
	}
	result, err := service.Run(ctx, scheduled.Job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if result.(*replayDependencyOutput).Data[0].ID != 7 {
		t.Fatal("ambient query overrode durable external value")
	}
}
