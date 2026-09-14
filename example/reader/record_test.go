package reader

import (
	"reflect"
	"testing"

	dtag "github.com/viant/datly/tag"
)

func TestRecordMetadataParses(t *testing.T) {
	for _, contract := range []reflect.Type{
		reflect.TypeOf(RecordInput{}),
		reflect.TypeOf(RecordOutput{}),
	} {
		for index := 0; index < contract.NumField(); index++ {
			if _, err := dtag.ParseField(contract.Field(index)); err != nil {
				t.Fatalf("%s.%s metadata: %v", contract.Name(), contract.Field(index).Name, err)
			}
		}
	}

	field, ok := reflect.TypeOf(Components{}).FieldByName("Record")
	if !ok {
		t.Fatal("Record component declaration is missing")
	}
	component, found, err := dtag.ParseComponent(field.Tag)
	if err != nil {
		t.Fatalf("parse Record component: %v", err)
	}
	if !found || component.Method != "GET" || component.Path != "/v1/api/records/{id}" {
		t.Fatalf("unexpected Record component metadata: %+v", component)
	}
}
