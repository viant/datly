package tag

import "testing"

func TestRelationOutputRoundTrip(t *testing.T) {
	value := "ID:p.id=HiddenKey:items.ORDER_ID|parent_key"
	links, err := ParseRelation(value)
	if err != nil {
		t.Fatal(err)
	}
	if links[0].Child.Field != "HiddenKey" || links[0].Child.Column != "ORDER_ID" || links[0].Child.Output != "parent_key" {
		t.Fatalf("%+v", links[0].Child)
	}
	rendered, err := RelationValue(links)
	if err != nil || rendered != value {
		t.Fatalf("%s %v", rendered, err)
	}
	for _, value := range []string{"id=key|", "id=key|a|b"} {
		if _, err := ParseRelation(value); err == nil {
			t.Fatalf("accepted %s", value)
		}
	}
}
