package transcribe

import (
	"context"
	"embed"
	"io/fs"
	"strings"
	"testing"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
)

//go:embed testdata/sql/*.sql
var sourceTestResources embed.FS

func TestResolveComponentSourcesExpandsSQLFromEmbedFS(t *testing.T) {
	resources, err := fs.Sub(sourceTestResources, "testdata")
	if err != nil {
		t.Fatalf("fs.Sub() error = %v", err)
	}
	source := `#setting($_ = $route('/events', 'GET'))
SELECT * FROM (${embed:sql/events.sql}) events`
	component := compileResourceComponent(t, source)
	if err = resolveComponentSources(component, resources); err != nil {
		t.Fatalf("resolveComponentSources() error = %v", err)
	}
	actual := component.RootSource().SQL
	if !strings.Contains(actual, "SELECT id, name FROM events") || strings.Contains(actual, "${embed:") {
		t.Fatalf("Component SQL = %q", actual)
	}
}

func TestResolveComponentSourcesExpandsNamespacedSQLFromSharedStore(t *testing.T) {
	resources := resource.New()
	if err := resources.Register("component", sourceTestResources); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	source := `#setting($_ = $route('/events', 'GET'))
SELECT * FROM (${embed:component:testdata/sql/events.sql}) events`
	component := compileResourceComponent(t, source)
	if err := resolveComponentSources(component, resources); err != nil {
		t.Fatalf("resolveComponentSources() error = %v", err)
	}
	actual := component.RootSource().SQL
	if !strings.Contains(actual, "SELECT id, name FROM events") || strings.Contains(actual, "${embed:") {
		t.Fatalf("Component SQL = %q", actual)
	}
}

func TestResolveComponentSourcesExpandsChildViewSQLFromEmbedFS(t *testing.T) {
	resources, err := fs.Sub(sourceTestResources, "testdata")
	if err != nil {
		t.Fatalf("fs.Sub() error = %v", err)
	}
	source := `#setting($_ = $route('/events', 'GET'))
SELECT o.*, i.*
FROM events o
JOIN (${embed:sql/events.sql}) i ON i.id = o.id`
	component := compileResourceComponent(t, source)
	if err = resolveComponentSources(component, resources); err != nil {
		t.Fatalf("resolveComponentSources() error = %v", err)
	}
	child := component.RootView.Relations[0].View.Source
	if child == nil || !strings.Contains(child.SQL, "SELECT id, name FROM events") ||
		strings.Contains(child.SQL, "${embed:") || len(child.Embeds) != 0 {
		t.Fatalf("child source = %+v", child)
	}
}

func TestResolveComponentSourcesExpandsIndependentViewSQLFromEmbedFS(t *testing.T) {
	resources, err := fs.Sub(sourceTestResources, "testdata")
	if err != nil {
		t.Fatalf("fs.Sub() error = %v", err)
	}
	source := `#setting($_ = $route('/events', 'GET'))
#define($_ = $Existing<[]Event>(view/existing) /* SELECT * FROM (${embed:sql/events.sql}) e */)
SELECT 1`
	component := compileResourceComponent(t, source)
	if err = resolveComponentSources(component, resources); err != nil {
		t.Fatalf("resolveComponentSources() error = %v", err)
	}
	if len(component.Views) != 1 || component.Views[0].Source == nil ||
		!strings.Contains(component.Views[0].Source.SQL, "SELECT id, name FROM events") ||
		strings.Contains(component.Views[0].Source.SQL, "${embed:") || len(component.Views[0].Source.Embeds) != 0 {
		t.Fatalf("independent source = %+v", component.Views)
	}
}

func TestResolveComponentSourcesExpandsRepeatedResourceToken(t *testing.T) {
	resources, err := fs.Sub(sourceTestResources, "testdata")
	if err != nil {
		t.Fatalf("fs.Sub() error = %v", err)
	}
	source := `#setting($_ = $route('/events', 'GET'))
SELECT a.*, b.*
FROM (${embed:sql/events.sql}) a
JOIN (${embed:sql/events.sql}) b ON b.id = a.id`
	component := compileResourceComponent(t, source)
	if err = resolveComponentSources(component, resources); err != nil {
		t.Fatalf("resolveComponentSources() error = %v", err)
	}
	root := component.RootView.Source
	child := component.RootView.Relations[0].View.Source
	if strings.Contains(root.SQL, "${embed:") || strings.Contains(child.SQL, "${embed:") ||
		len(root.Embeds) != 0 || len(child.Embeds) != 0 {
		t.Fatalf("root = %+v, child = %+v", root, child)
	}
}

func TestResolveComponentSourcesRejectsMissingDeclaredResource(t *testing.T) {
	source := `#setting($_ = $route('/events', 'GET'))
SELECT * FROM (${embed:sql/missing.sql}) events`
	component := compileResourceComponent(t, source)
	if err := resolveComponentSources(component, sourceTestResources); err == nil || !strings.Contains(err.Error(), `read SQL resource "sql/missing.sql"`) {
		t.Fatalf("resolveComponentSources() error = %v", err)
	}
}

func compileResourceComponent(t *testing.T, source string) *spec.Component {
	t.Helper()
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/demo",
		Name:  "Events",
		Text:  source,
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return result.Component
}
