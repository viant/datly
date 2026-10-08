package compile

import (
	"github.com/viant/datly/spec"
	"testing"
	"testing/fstest"
)

func TestBackfillLinkedMetadataFromResources(t *testing.T) {
	resources := fstest.MapFS{"parent.sql": &fstest.MapFile{Data: []byte("SELECT p.id AS parent_id FROM parents p")}, "child.sql": &fstest.MapFile{Data: []byte("SELECT c.owner_id AS parent_key FROM children c")}}
	component := &spec.Component{Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}}, RootView: &spec.View{Name: "Parents", Namespace: "p", Source: &spec.ViewSource{URI: "parent.sql"}, Columns: []*spec.Column{{Name: "ID", Source: "parent_id", NameInferred: true}}, Relations: []*spec.Relation{{Name: "Children", Holder: "Children", View: &spec.View{Name: "Children", Namespace: "c", Source: &spec.ViewSource{URI: "child.sql"}, Columns: []*spec.Column{{Name: "Owner", Source: "parent_key"}}}, On: []*spec.RelationLink{{ParentColumn: "parent_id", ChildColumn: "parent_key"}}}}}}
	if err := BackfillRelationMetadata(component, resources); err != nil {
		t.Fatal(err)
	}
	if err := BackfillReportMetadata(component, resources); err != nil {
		t.Fatal(err)
	}
	link := component.RootView.Relations[0].On[0]
	if link.ChildField != "Owner" || link.ChildColumn != "owner_id" || link.ChildNamespace != "c" || link.ChildOutput != "parent_key" {
		t.Fatalf("%+v", link)
	}
	if component.RootView.Columns[0].Output != "parent_id" {
		t.Fatalf("%+v", component.RootView.Columns[0])
	}
	if component.RootView.Source.SQL != "" || component.RootView.Relations[0].View.Source.SQL != "" {
		t.Fatal("authoring replaced resource contract")
	}
	// A second generation pass uses complete metadata without reopening resources.
	if err := BackfillRelationMetadata(component); err != nil {
		t.Fatal(err)
	}
	if err := BackfillReportMetadata(component); err != nil {
		t.Fatal(err)
	}
}
