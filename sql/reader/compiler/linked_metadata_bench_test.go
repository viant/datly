package compiler

import (
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/xunsafe"
	"reflect"
	"testing"
)

func linkedProjectionFixture(sql string) (*data.View, *data.Link) {
	link := data.NewLink("items", "ORDER_ID", "ParentKey")
	child := &data.View{Spec: spec.View{Name: "Items", Namespace: "items", Source: &spec.ViewSource{SQL: sql}}, Columns: []*data.Column{{Name: "ParentKey", Column: "parent_key", Tag: `sqlx:"parent_key"`}}}
	return &data.View{Relations: []*data.Relation{{Name: "Items", Of: &data.RelationRef{View: child, On: data.Links{link}}}}}, link
}

func TestLinkedRelationExplicitMetadata(t *testing.T) {
	root, link := linkedProjectionFixture("SELECT items.ORDER_ID AS parent_key FROM ITEMS items")
	if err := resolveRelationProjections(root); err != nil {
		t.Fatal(err)
	}
	if link.Column != "ORDER_ID" || link.OutputColumn() != "parent_key" || link.Field != "ParentKey" {
		t.Fatalf("link=%+v", link)
	}
}

func TestLinkedRelationOpaqueSQLMetadata(t *testing.T) {
	root, link := linkedProjectionFixture("SELECT FROM vendor_specific_source")
	if err := resolveRelationProjections(root); err != nil {
		t.Fatal(err)
	}
	if link.Column != "ORDER_ID" || link.OutputColumn() != "parent_key" {
		t.Fatalf("metadata must be independent of SQL: %+v", link)
	}
}

func BenchmarkLinkedRelationProjection(b *testing.B) {
	root, link := linkedProjectionFixture("SELECT items.ORDER_ID AS parent_key FROM ITEMS items")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		link.Column, link.Output = "ORDER_ID", ""
		if err := resolveRelationProjections(root); err != nil {
			b.Fatal(err)
		}
	}
}

type linkedBenchRow struct {
	ID        int    `sqlx:"id"`
	ParentKey int    `sqlx:"parent_key"`
	Name      string `sqlx:"name"`
}

var linkedFieldSink any

func BenchmarkLinkedFieldMetadata(b *testing.B) {
	typ := reflect.TypeFor[linkedBenchRow]()
	b.Run("reflect_visible", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			linkedFieldSink = reflect.VisibleFields(typ)
		}
	})
	b.Run("xunsafe_struct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			linkedFieldSink = xunsafe.NewStruct(typ)
		}
	})
}

func TestLinkedRelationKeepsPredicateScope(t *testing.T) {
	root, link := linkedProjectionFixture("SELECT FROM vendor_source")
	link.Column = "ParentKey"
	if err := resolveRelationProjections(root); err != nil {
		t.Fatal(err)
	}
	if link.Column != "ParentKey" || link.OutputColumn() != "parent_key" {
		t.Fatalf("predicate source overwritten: %+v", link)
	}
}
