package write

import (
	"reflect"
	"testing"

	dtag "github.com/viant/datly/tag"
	xhandler "github.com/viant/xdatly/handler"
)

func TestRecordPatchMetadataParses(t *testing.T) {
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

	field, ok := reflect.TypeOf(Components{}).FieldByName("RecordPatch")
	if !ok {
		t.Fatal("RecordPatch component declaration is missing")
	}
	component, found, err := dtag.ParseComponent(field.Tag)
	if err != nil {
		t.Fatalf("parse RecordPatch component: %v", err)
	}
	if !found || component.Method != "PATCH" || component.Path != "/v1/api/records" ||
		component.Handler != "NewRecordPatchHandler" {
		t.Fatalf("unexpected RecordPatch component metadata: %+v", component)
	}

	handler := NewRecordPatchRuntimeHandler(xhandler.Capabilities{})
	if handler.InputType() != reflect.TypeOf(RecordInput{}) || handler.OutputType() != reflect.TypeOf(RecordOutput{}) {
		t.Fatalf("runtime handler contract = %v -> %v", handler.InputType(), handler.OutputType())
	}
}
