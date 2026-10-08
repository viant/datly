package generate

import (
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	reader "github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestGeneratedRelationMetadataReload(t *testing.T) {
	child := &spec.View{Name: "Items", Namespace: "items", Columns: []*spec.Column{{Name: "ParentKey", Source: "parent_key", Type: spec.TypeRef{Name: "int"}}}, Source: &spec.ViewSource{SQL: "SELECT items.ORDER_ID AS parent_key FROM ITEMS items"}}
	root := &spec.View{Name: "Orders", Namespace: "orders", Columns: []*spec.Column{{Name: "ID", Source: "ID", Type: spec.TypeRef{Name: "int"}}}, Source: &spec.ViewSource{SQL: "SELECT orders.ID FROM ORDERS orders"}, Relations: []*spec.Relation{{Name: "Items", Holder: "Items", View: child, On: []*spec.RelationLink{{ParentNamespace: "orders", ParentColumn: "ID", ChildNamespace: "items", ChildColumn: "parent_key"}}}}}
	plan, err := New(Input{Component: &spec.Component{Name: "Orders", RootView: root}}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	var holder Field
	var key Field
	for _, v := range plan.Views {
		for _, f := range v.Fields {
			if f.Name == "Items" {
				holder = f
			}
			if f.Name == "ParentKey" {
				key = f
			}
		}
	}
	links, err := tag.ParseRelation(reflect.StructTag(holder.Tag).Get("on"))
	if err != nil || len(links) != 1 {
		t.Fatalf("%+v %v", holder, err)
	}
	if links[0].Child.Column != "ORDER_ID" || links[0].Child.Output != "parent_key" || links[0].Child.Field != "ParentKey" {
		t.Fatalf("%+v", links[0].Child)
	}
	row := reflect.StructOf([]reflect.StructField{{Name: "ParentKey", Type: reflect.TypeFor[int](), Tag: reflect.StructTag(key.Tag)}})
	parent := reflect.StructOf([]reflect.StructField{{Name: "ID", Type: reflect.TypeFor[int]()}, {Name: "Items", Type: reflect.SliceOf(reflect.PointerTo(row)), Tag: reflect.StructTag(holder.Tag)}})
	// Linked reload has no authored DQL and needs no source projection parsing.
	component := &spec.Component{Name: "Orders", RootView: &spec.View{Name: "Orders", Source: &spec.ViewSource{SQL: "SELECT orders.ID FROM ORDERS orders"}}}
	view, err := reader.CompileViewMetadata(reader.Input{Component: component, OutputType: reflect.SliceOf(reflect.PointerTo(parent))})
	if err != nil {
		t.Fatal(err)
	}
	link := view.Relations[0].Of.On[0]
	if link.Field != "ParentKey" || link.Column != "ORDER_ID" || link.OutputColumn() != "parent_key" {
		t.Fatalf("%+v", link)
	}
	if root.Relations[0].On[0].ChildOutput != "" {
		t.Fatal("generation mutated caller contract")
	}
}

func TestGeneratedReportMetadataReload(t *testing.T) {
	groupable := true
	component := &spec.Component{Name: "Metrics", Settings: &spec.Settings{Report: &spec.ReportSettings{Enabled: true}}, RootView: &spec.View{Name: "Metrics", Source: &spec.ViewSource{SQL: "SELECT account_id AS public_id FROM spend"}, Columns: []*spec.Column{{Name: "public_id", Source: "account_id", Type: spec.TypeRef{Name: "int"}, Groupable: &groupable}}}}
	plan, err := New(Input{Component: component}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	var field Field
	for _, v := range plan.Views {
		for _, candidate := range v.Fields {
			if candidate.Name == "PublicId" || candidate.Name == "PublicID" {
				field = candidate
			}
		}
	}
	if field.Name == "" {
		t.Fatalf("no generated output: %+v", plan.Views)
	}
	if reflect.StructTag(field.Tag).Get("sqlOutput") != "public_id" {
		t.Fatalf("missing output proof: %s", field.Tag)
	}
	// Reconstruct only the linked metadata and use SQL that cannot be parsed.
	view := &spec.View{Name: "Metrics", Source: &spec.ViewSource{SQL: "SELECT FROM vendor_source"}, Columns: []*spec.Column{{Name: "public_id", Source: "account_id", Tag: field.Tag}}}
	columns, err := (bootstrap.ViewProjection{View: view}).Columns()
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) != 1 || columns[0].Name != "public_id" || columns[0].Selector != "public_id" {
		t.Fatalf("%+v", columns)
	}
}
