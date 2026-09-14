package typecatalog

import (
	"reflect"
	"testing"
)

func TestFieldByName(t *testing.T) {
	type sample struct {
		VendorID  int
		FirstName string
	}
	tests := []struct {
		name     string
		input    string
		want     string
		resolved bool
	}{
		{name: "exact", input: "VendorID", want: "VendorID", resolved: true},
		{name: "preserve acronym", input: "vendorID", want: "VendorID", resolved: true},
		{name: "format", input: "first_name", want: "FirstName", resolved: true},
		{name: "empty", input: "", resolved: false},
		{name: "whitespace", input: "  ", resolved: false},
		{name: "missing", input: "unknown", resolved: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			field, ok := FieldByName(reflect.TypeOf((*sample)(nil)), test.input)
			if ok != test.resolved || field.Name != test.want {
				t.Fatalf("FieldByName(%q) = %q, %v", test.input, field.Name, ok)
			}
		})
	}
}

func TestFieldName(t *testing.T) {
	if actual := FieldName("order_items"); actual != "OrderItems" {
		t.Fatalf("FieldName() = %q", actual)
	}
	if actual := ExportedFieldName("AccountIDs"); actual != "AccountIDs" {
		t.Fatalf("ExportedFieldName() did not preserve exported acronym = %q", actual)
	}
}
