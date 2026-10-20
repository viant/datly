package compile

import (
	"testing"

	"github.com/viant/datly/spec"
)

func TestRelationDeclaredOutputWinsOverPhysicalSQLXMapping(t *testing.T) {
	child := &spec.View{Name: "carrier", Namespace: "carrier", Source: &spec.ViewSource{SQL: "SELECT * FROM (SELECT c.NAME AS CARRIER_VALUE,c.NAME AS NAME FROM carriers c) carrier"}, Columns: []*spec.Column{
		{Name: "CarrierValue", Source: "NAME", Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"NAME|CARRIER_VALUE"`},
		{Name: "Name", Source: "NAME", Type: spec.TypeRef{Name: "string"}, Tag: `sqlx:"NAME"`},
	}}
	relation := &spec.Relation{Name: "Carrier", Holder: "Carrier", View: child, On: []*spec.RelationLink{{ParentNamespace: "audience", ParentColumn: "CARRIER_VALUES", ChildNamespace: "carrier", ChildColumn: "CARRIER_VALUE", ChildField: "CarrierValue"}}}
	root := &spec.View{Name: "audience", Namespace: "audience", Source: &spec.ViewSource{SQL: "SELECT CARRIER_VALUES FROM audience"}, Columns: []*spec.Column{{Name: "CarrierValues", Source: "CARRIER_VALUES", Type: spec.TypeRef{Name: "string"}}}, Relations: []*spec.Relation{relation}}
	if err := BackfillRelationMetadata(&spec.Component{Name: "Audience", RootView: root}); err != nil {
		t.Fatal(err)
	}
	link := relation.On[0]
	if link.ChildColumn != "CARRIER_VALUE" || link.ChildOutput != "CARRIER_VALUE" || link.ChildField != "CarrierValue" {
		t.Fatalf("declared relation output redirected: %+v", link)
	}
}
