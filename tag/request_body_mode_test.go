package tag

import (
	"reflect"
	"testing"
)

func TestRequestBodyModeComponentTagRoundtrip(t *testing.T) {
	c := Component{Name: "Delete", Path: "/views/{id}", Method: "DELETE", RequestBodyMode: "on_demand"}
	raw, err := c.StructTag()
	if err != nil {
		t.Fatal(err)
	}
	got, present, err := ParseComponent(reflect.StructTag(raw))
	if err != nil || !present || got.RequestBodyMode != c.RequestBodyMode {
		t.Fatalf("mode roundtrip: %v %v", got, err)
	}
	c.RequestBodyMode = "invalid"
	if _, err := c.StructTag(); err == nil {
		t.Fatal("invalid tag mode accepted")
	}
}
