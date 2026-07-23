package openapi

import (
	"reflect"
	"testing"
)

type tagParseNamed struct {
	A int
}

type tagParseFixture struct {
	Values []int         `json:"values"`
	Any    tagParseNamed `json:"any"`
	Hidden string        `json:"hidden" internal:"true"`
	Summary string       `json:"summary" parameter:"kind=output,in=summary"`
	ViewOut string       `json:"view_out" parameter:"kind=output,in=view"`
	InputDrop string     `json:"input_drop" parameter:"kind=query,in=id"`
	ByViewTable string   `json:"by_view" view:"name=V,table=orders"`
	BySQLX string        `json:"by_sqlx" sqlx:"name=ORD_ID"`
}

func TestParseTag(t *testing.T) {
	rType := reflect.TypeOf(tagParseFixture{})
	tests := []struct {
		name             string
		fieldIndex       int
		isInput          bool
		rootTable        string
		expectIgnore     bool
		expectTypeName   string
		expectJSONName   string
		expectNullable   bool
		expectTableValue string
	}{
		{name: "slice sets json name", fieldIndex: 0, isInput: false, rootTable: "", expectIgnore: false, expectTypeName: "", expectJSONName: "values", expectNullable: false},
		{name: "struct sets type name", fieldIndex: 1, isInput: false, rootTable: "", expectIgnore: false, expectTypeName: "openapi.tagParseNamed", expectJSONName: "any", expectNullable: false},
		{name: "internal flag ignored", fieldIndex: 2, isInput: false, rootTable: "", expectIgnore: true, expectTypeName: "", expectJSONName: "hidden", expectNullable: false},
		{name: "output summary table", fieldIndex: 3, isInput: false, rootTable: "root", expectIgnore: false, expectTypeName: "", expectJSONName: "summary", expectNullable: false, expectTableValue: "SUMMARY"},
		{name: "output view table", fieldIndex: 4, isInput: false, rootTable: "root", expectIgnore: false, expectTypeName: "", expectJSONName: "view_out", expectNullable: false, expectTableValue: "root"},
		{name: "input non body ignored", fieldIndex: 5, isInput: true, rootTable: "root", expectIgnore: true, expectTypeName: "", expectJSONName: "input_drop", expectNullable: false},
		{name: "view tag table", fieldIndex: 6, isInput: false, rootTable: "root", expectIgnore: false, expectTypeName: "", expectJSONName: "by_view", expectNullable: false, expectTableValue: "orders"},
		{name: "sqlx column captured", fieldIndex: 7, isInput: false, rootTable: "root", expectIgnore: false, expectTypeName: "", expectJSONName: "by_sqlx", expectNullable: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := rType.Field(tt.fieldIndex)
			parsed, err := ParseTag(field, field.Tag, tt.isInput, tt.rootTable)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if parsed.Ignore != tt.expectIgnore {
				t.Fatalf("expected ignore %v, got %v", tt.expectIgnore, parsed.Ignore)
			}
			if parsed.TypeName != tt.expectTypeName {
				t.Fatalf("expected type name %q, got %q", tt.expectTypeName, parsed.TypeName)
			}
			if parsed.JSONName != tt.expectJSONName {
				t.Fatalf("expected json name %q, got %q", tt.expectJSONName, parsed.JSONName)
			}
			if parsed.IsNullable != tt.expectNullable {
				t.Fatalf("expected nullable %v, got %v", tt.expectNullable, parsed.IsNullable)
			}
			if parsed.Table != tt.expectTableValue {
				t.Fatalf("expected table %q, got %q", tt.expectTableValue, parsed.Table)
			}
			if tt.name == "sqlx column captured" && parsed.Column != "ORD_ID" {
				t.Fatalf("expected column ORD_ID, got %q", parsed.Column)
			}
		})
	}
}
