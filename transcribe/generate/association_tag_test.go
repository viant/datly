package generate

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestAssociationHolderUsesDeclaredMemberAndParentLink(t *testing.T) {
	child := &spec.View{Name: "order_creative", Columns: []*spec.Column{
		{Name: "Id"}, {Name: "CreativeId"}, {Name: "AdOrderId"},
	}}
	p := &viewPlanner{names: map[*spec.View]string{child: "OrderCreative"}}
	rel := &spec.Relation{
		Name: "order_creative", Holder: "Creatives", View: child,
		Tag: `association:"collection=creativeIds,member=CreativeId"`,
		On:  []*spec.RelationLink{{ParentField: "Id", ParentColumn: "ID", ChildField: "AdOrderId", ChildColumn: "AD_ORDER_ID"}},
	}
	field, err := p.relationField(rel)
	if err != nil {
		t.Fatal(err)
	}
	tag := reflect.StructTag(field.Tag)
	if tag.Get("association") != "collection=creativeIds,member=CreativeId" || tag.Get("on") == "" {
		t.Fatalf("incomplete generated relation %s", field.Tag)
	}
	rel.Tag = `association:"collection=creativeIds,member=Typo"`
	if _, err = p.relationField(rel); err == nil {
		t.Fatal("generated relation with an unknown member")
	}
}
