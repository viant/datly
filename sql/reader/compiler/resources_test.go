package compiler

import (
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/spec"
)

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
