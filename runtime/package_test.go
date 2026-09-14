package runtime

import (
	"context"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/runtime/route"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
)

type packageReaderInput struct {
	Tenant int `parameter:"Tenant,kind=query,in=tenant"`
}

type packageReaderRow struct {
	ID   int
	Name string
}

type packageReaderOutput struct {
	Data []*packageReaderRow `parameter:",kind=output,in=view" view:"Users,table=users" sql:"SELECT id, name FROM users WHERE tenant = :Tenant ORDER BY id"`
}

func TestPackageTagsBootstrapUnifiedReader(t *testing.T) {
	h := testharness.NewSQLiteHarness(t)
	if err := h.ExecStatements(context.Background(),
		`CREATE TABLE users (id INTEGER PRIMARY KEY, tenant INTEGER, name TEXT)`,
		`INSERT INTO users(id, tenant, name) VALUES (1, 7, 'one'), (2, 7, 'two'), (3, 8, 'other')`,
	); err != nil {
		t.Fatalf("setup failed: %v", err)
	}
	authored := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/package", Name: "Users"}, Name: "Users",
		Routes: []*spec.Route{{Method: http.MethodGet, Path: "/package/users"}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{
		Component: authored, InputType: reflect.TypeOf(packageReaderInput{}), OutputType: reflect.TypeOf(packageReaderOutput{}),
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	if authored.RootView != nil || len(authored.Parameters) != 0 {
		t.Fatal("package bootstrap mutated authored component metadata")
	}
	reader, err := sqlreader.NewExecution(sqlreader.Config{
		Component: artifact.Component, InputType: reflect.TypeOf(packageReaderInput{}), OutputType: reflect.TypeOf(packageReaderOutput{}),
		Plan: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB},
	})
	if err != nil {
		t.Fatalf("NewExecution() error = %v", err)
	}
	bundle, err := route.NewBundle([]*spec.Component{artifact.Component})
	if err != nil {
		t.Fatalf("NewBundle() error = %v", err)
	}
	service := newTestService(t, bundle, map[string]*registry.RegisteredComponent{
		artifact.Component.Key.String(): {
			Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(packageReaderOutput{}), Reader: reader,
		},
	})
	actual, err := executeTestRoute(t, service, context.Background(), testharness.NewRequest(http.MethodGet, "/package/users").WithQuery(url.Values{"tenant": {"7"}}))
	if err != nil {
		t.Fatalf("ExecuteRoute() error = %v", err)
	}
	output := actual.(*packageReaderOutput)
	if len(output.Data) != 2 || output.Data[0].ID != 1 || output.Data[1].ID != 2 {
		t.Fatalf("Data = %#v", output.Data)
	}
}
