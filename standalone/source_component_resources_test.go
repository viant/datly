package standalone

import (
	"context"
	"embed"
	"io/fs"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/report"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/standalone/config"
	"github.com/viant/datly/typecatalog"
)

func TestReaderConfigureBindsConnectorCapabilityForPredicates(t *testing.T) {
	input, err := testReflectedArtifactInput(testResourceComponent("A", "testdata/eager_overlap_a/query.sql"),
		reflect.TypeOf(eagerOverlapAInput{}), reflect.TypeOf(eagerOverlapOutput{}), resource.New())
	if err != nil {
		t.Fatal(err)
	}
	compilation, err := report.NewProjectCompiler(report.ProjectConfig{Types: typecatalog.NewCatalog()}).CompileArtifacts([]bootstrap.ArtifactInput{input})
	if err != nil {
		t.Fatal(err)
	}
	artifacts := compilation.Artifacts()
	if len(artifacts) != 1 {
		t.Fatalf("reader artifacts=%d, want 1", len(artifacts))
	}
	sqlConnector := &dsql.SQLComponent{}
	configured := &sourceComponent{source: &source{config: &config.Config{}, connections: &connector.Set{SQL: sqlConnector}}}
	capabilities, err := configured.Configure(context.Background(), artifacts[0])
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.Invocation.Connector != sqlConnector {
		t.Fatal("reader predicate lost the configured SQL connector capability")
	}
}

//go:embed testdata/eager_resources/root.sql testdata/eager_resources/child.sql
var eagerComponentResources embed.FS

//go:embed testdata/eager_overlap_a/query.sql
var eagerOverlapAResources embed.FS

//go:embed testdata/eager_overlap_b/query.sql
var eagerOverlapBResources embed.FS

type eagerResourceInput struct{}

func (eagerResourceInput) EmbedFS() *embed.FS { return &eagerComponentResources }

type eagerResourceChild struct {
	ID     int `sqlx:"id"`
	UserID int `sqlx:"user_id"`
}

type eagerResourceRow struct {
	ID       int                  `sqlx:"id"`
	Children []eagerResourceChild `view:"children" sql:"uri=testdata/eager_resources/child.sql" on:"ID:id=UserID:user_id"`
}

type eagerResourceOutput struct {
	Data []eagerResourceRow
}

type eagerOverlapAInput struct{}

func (eagerOverlapAInput) EmbedFS() *embed.FS { return &eagerOverlapAResources }

type eagerOverlapBInput struct{}

func (eagerOverlapBInput) EmbedFS() *embed.FS { return &eagerOverlapBResources }

type eagerOverlapOutput struct {
	Data []struct {
		ID int `sqlx:"id"`
	}
}

func TestReflectedArtifactInputUsesInputEmbedFSForRootAndChildSQL(t *testing.T) {
	component := &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/app/eager", Name: "Users"},
		Routes: []*spec.Route{{Method: "GET", Path: "/users"}},
		RootView: &spec.View{Name: "users", Source: &spec.ViewSource{
			URI: "testdata/eager_resources/root.sql",
		}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	input, err := testReflectedArtifactInput(component, reflect.TypeOf(eagerResourceInput{}), reflect.TypeOf(eagerResourceOutput{}), resource.New())
	if err != nil {
		t.Fatalf("reflectedArtifactInput() error = %v", err)
	}
	artifact, err := bootstrap.BuildArtifact(input)
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	rootSQL := strings.TrimSpace(artifact.Reader.Root.View.Spec.Source.SQL)
	if rootSQL != "SELECT id FROM users" {
		t.Fatalf("root SQL = %q", rootSQL)
	}
	if len(artifact.Reader.Root.View.Relations) != 1 || artifact.Reader.Root.View.Relations[0].Of == nil {
		t.Fatalf("relations = %#v", artifact.Reader.Root.View.Relations)
	}
	childSQL := strings.TrimSpace(artifact.Reader.Root.View.Relations[0].Of.View.Spec.Source.SQL)
	if childSQL != "SELECT id, user_id FROM user_children" {
		t.Fatalf("child SQL = %q", childSQL)
	}
}

func TestReflectedArtifactInputEmbedFSDefaultIsComponentScoped(t *testing.T) {
	global := resource.New()
	a, err := testReflectedArtifactInput(testResourceComponent("A", "testdata/eager_overlap_a/query.sql"), reflect.TypeOf(eagerOverlapAInput{}), reflect.TypeOf(eagerOverlapOutput{}), global)
	if err != nil {
		t.Fatalf("component A reflectedArtifactInput() error = %v", err)
	}
	b, err := testReflectedArtifactInput(testResourceComponent("B", "testdata/eager_overlap_b/query.sql"), reflect.TypeOf(eagerOverlapBInput{}), reflect.TypeOf(eagerOverlapOutput{}), global)
	if err != nil {
		t.Fatalf("component B reflectedArtifactInput() error = %v", err)
	}
	if _, ok := global.Lookup(""); ok {
		t.Fatal("input EmbedFS was registered as the global empty namespace")
	}
	assertResolvedRootSQL(t, a, "SELECT id FROM component_a")
	assertResolvedRootSQL(t, b, "SELECT id FROM component_b")
	if _, err := fs.ReadFile(a.Resources, "testdata/eager_overlap_b/query.sql"); err == nil {
		t.Fatal("component A resource store can read component B local SQL")
	}
	if _, err := fs.ReadFile(b.Resources, "testdata/eager_overlap_a/query.sql"); err == nil {
		t.Fatal("component B resource store can read component A local SQL")
	}
}

func testReflectedArtifactInput(component *spec.Component, inputType, outputType reflect.Type, resources *resource.Store) (bootstrap.ArtifactInput, error) {
	source := &sourceComponent{source: &source{config: &config.Config{}}}
	return source.reflectedArtifactInput(component, &bootstrap.RouteSource{LinkedInputType: inputType, LinkedOutputType: outputType}, typecatalog.NewCatalog(), resources)
}

func testResourceComponent(name, uri string) *spec.Component {
	return &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/app/eager", Name: name},
		Routes: []*spec.Route{{Method: "GET", Path: "/" + name}},
		RootView: &spec.View{Name: name, Source: &spec.ViewSource{
			URI: uri,
		}},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
}

func assertResolvedRootSQL(t *testing.T, input bootstrap.ArtifactInput, expected string) {
	t.Helper()
	artifact, err := bootstrap.BuildArtifact(input)
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	if got := strings.TrimSpace(artifact.Reader.Root.View.Spec.Source.SQL); got != expected {
		t.Fatalf("root SQL = %q, want %q", got, expected)
	}
}
