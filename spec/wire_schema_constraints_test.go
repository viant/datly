package spec

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestWireSchemaObjectConstraintClonePersistence(t *testing.T) {
	closed := false
	source := &WireSchema{Required: true, MinItems: 1, MaxItems: 50, Items: &WireSchema{Required: true, AdditionalProperties: &closed, AnyOf: []*WireSchema{{RequiredFields: []string{"id"}}, {RequiredFields: []string{"name", "owner"}}}}}
	encoded, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var restored WireSchema
	if err = json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(source, &restored) {
		t.Fatalf("persistence: %s", encoded)
	}
	cloned := restored.Clone()
	cloned.Items.AnyOf[0].RequiredFields[0] = "changed"
	*cloned.Items.AdditionalProperties = true
	if restored.Items.AnyOf[0].RequiredFields[0] != "id" || *restored.Items.AdditionalProperties {
		t.Fatal("clone shares object constraint metadata")
	}
}
