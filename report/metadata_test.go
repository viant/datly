package report

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestMetadataCompileRelationHoldersMatchesQualifiedLinkColumns(t *testing.T) {
	view := &spec.View{
		Columns: []*spec.Column{
			{Name: "channelId", Source: "channel_id"},
			{Name: "publisherId", Source: "p.publisher_id"},
			{Name: "site_id"},
		},
		Relations: []*spec.Relation{
			{
				Holder: "SupplyChannel",
				On: []*spec.RelationLink{
					{ParentColumn: "supply_performance.channel_id", ChildColumn: "ID"},
				},
			},
			{
				Holder: "SupplyPublisher",
				On: []*spec.RelationLink{
					{ParentColumn: "`publisher_id`", ChildColumn: "ID"},
				},
			},
			{
				Holder: "SiteInfo",
				On: []*spec.RelationLink{
					{ParentColumn: "LOWER(site_id)", ChildColumn: "ID"},
				},
			},
		},
	}
	meta := &metadata{
		dimensions: []field{
			{name: "channelId", publicName: "channelId"},
			{name: "publisherId", publicName: "publisherId"},
			{name: "site_id", publicName: "site_id"},
		},
		holders: map[string][]string{},
	}

	meta.compileRelationHolders(view)

	expected := map[string][]string{
		"channelId":   []string{"SupplyChannel"},
		"publisherId": []string{"SupplyPublisher"},
	}
	if !reflect.DeepEqual(meta.holders, expected) {
		t.Fatalf("holders = %+v, want %+v", meta.holders, expected)
	}
}
