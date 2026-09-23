package compiler

import (
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestResolveInMemoryDiscoverySource(t *testing.T) {
	child := &spec.View{Name: "signals", InMemory: true,
		Source: &spec.ViewSource{URI: "missing-discovery.sql", Table: "placeholder"},
		Relations: []*spec.Relation{{Name: "perf", Holder: "Perf", View: &spec.View{
			Name: "perf", Source: &spec.ViewSource{URI: "perf.sql"},
		}}},
	}
	root := &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id FROM parents"},
		Relations: []*spec.Relation{{Name: "signals", Holder: "Signals", View: child}},
	}
	runtimeView := data.FromView(nil, root)
	err := resolveViewResources(runtimeView, fstest.MapFS{"perf.sql": {Data: []byte("SELECT feature_type, feature_value FROM performance")}})
	require.NoError(t, err)
	resolved := runtimeView.Relations[0].Of.View
	require.Empty(t, resolved.Spec.Source.SQL)
	require.Empty(t, resolved.Spec.Source.Table)
	require.Empty(t, resolved.Spec.Source.URI)
	require.Equal(t, "SELECT feature_type, feature_value FROM performance", resolved.Relations[0].Of.View.Spec.Source.SQL)
	require.Equal(t, "missing-discovery.sql", child.Source.URI, "runtime lowering must not destroy discovery metadata")
}

func TestResolveInMemoryRootRejected(t *testing.T) {
	root := data.FromView(nil, &spec.View{Name: "root", InMemory: true})
	require.ErrorContains(t, resolveViewResources(root, nil), "requires a parent relation")
}

func TestResolveViewResourcesExpandsInlineAndNestedURISources(t *testing.T) {
	type detail struct {
		ID     int
		UserID int
	}
	type row struct {
		ID      int
		Details []detail `view:"details" sql:"uri=component:sql/details.sql" on:"ID:id=UserID:user_id"`
	}
	type output struct {
		Data []row
	}
	component := &spec.Component{
		RootView: &spec.View{
			Name: "users",
			Source: &spec.ViewSource{
				SQL:    "SELECT id FROM (${embed:component:sql/users.sql}) users",
				Embeds: []*spec.EmbeddedSQLRef{{Path: "component:sql/users.sql", Raw: "${embed:component:sql/users.sql}"}},
			},
		},
		Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}},
	}
	resources := resource.New()
	if err := resources.Register("component", fstest.MapFS{
		"sql/users.sql":   {Data: []byte("SELECT id FROM users")},
		"sql/details.sql": {Data: []byte("SELECT id, user_id FROM details")},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	artifact, err := BuildArtifact(ArtifactInput{
		Component:       component,
		OutputType:      reflect.TypeOf(output{}),
		DirectViewField: "Data",
		Resources:       resources,
	})
	if err != nil {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
	if artifact.Reader.Root.View.Spec.Source.SQL != "SELECT id FROM (SELECT id FROM users) users" || len(artifact.Reader.Root.View.Spec.Source.Embeds) != 0 {
		t.Fatalf("root source = %+v", artifact.Reader.Root.View.Spec.Source)
	}
	if len(artifact.Reader.Root.View.Relations) != 1 || artifact.Reader.Root.View.Relations[0].Of == nil {
		t.Fatalf("relations = %#v", artifact.Reader.Root.View.Relations)
	}
	detailSource := artifact.Reader.Root.View.Relations[0].Of.View.Spec.Source
	if detailSource == nil || detailSource.SQL != "SELECT id, user_id FROM details" || len(detailSource.Embeds) != 0 {
		t.Fatalf("detail source = %+v", detailSource)
	}
	if component.RootView.Source.SQL != "SELECT id FROM (${embed:component:sql/users.sql}) users" || len(component.RootView.Source.Embeds) != 1 {
		t.Fatal("artifact compilation mutated component metadata")
	}
}

func TestResolveViewResourcesRequiresFilesystem(t *testing.T) {
	component := &spec.Component{RootView: &spec.View{Name: "users", Source: &spec.ViewSource{
		URI: "sql/users.sql",
	}}}
	_, err := BuildArtifact(ArtifactInput{Component: component})
	if err == nil || err.Error() != `resource filesystem is required for view "users"` {
		t.Fatalf("BuildArtifact() error = %v", err)
	}
}
