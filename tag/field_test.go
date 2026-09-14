package tag

import (
	"reflect"
	"testing"
)

func TestParseFieldDelegatesBindingAliasesToBindly(t *testing.T) {
	tests := []struct {
		name string
		tag  reflect.StructTag
	}{
		{name: "bind", tag: `bind:"vendorID,kind=path,in=vendorID,required"`},
		{name: "parameter", tag: `parameter:"vendorID,kind=path,in=vendorID,required"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata, err := ParseField(reflect.StructField{Name: "VendorID", Type: reflect.TypeOf(0), Tag: test.tag})
			if err != nil {
				t.Fatalf("ParseField() error = %v", err)
			}
			if metadata.Binding == nil || metadata.Binding.Name != "vendorID" || metadata.Binding.Location.Kind != "path" || metadata.Binding.Location.In != "vendorID" || metadata.Binding.Required == nil || !*metadata.Binding.Required {
				t.Fatalf("unexpected binding: %+v", metadata.Binding)
			}
		})
	}
}

func TestParseFieldRejectsAmbiguousBindingAliases(t *testing.T) {
	field := reflect.StructField{Name: "ID", Type: reflect.TypeOf(0), Tag: `bind:"kind=query,in=id" parameter:"kind=query,in=id"`}
	if _, err := ParseField(field); err == nil {
		t.Fatal("expected ambiguous bind/parameter error")
	}
}

func TestParseFieldMetadata(t *testing.T) {
	field := reflect.StructField{
		Name: "Children",
		Type: reflect.TypeOf([]string{}),
		Tag:  `view:"children,connector=analytics,batch=25" on:"ID=ParentID" sql:"uri=children.sql" codec:"AsStrings,trim" groupable:"true"`,
	}
	metadata, err := ParseField(field)
	if err != nil {
		t.Fatalf("ParseField() error = %v", err)
	}
	if metadata.View == nil || metadata.View.Name != "children" || metadata.View.Connector != "analytics" || metadata.View.Batch != 25 {
		t.Fatalf("unexpected view: %+v", metadata.View)
	}
	if metadata.SQL == nil || metadata.SQL.URI != "children.sql" || len(metadata.Relation) != 1 || metadata.Relation[0].Parent.Column != "ID" || metadata.Relation[0].Child.Column != "ParentID" || metadata.Codec == nil || !metadata.Groupable {
		t.Fatalf("unexpected metadata: %+v", metadata)
	}
}

func TestParseFieldRejectsNonCanonicalGroupableBoolean(t *testing.T) {
	field := reflect.StructField{Name: "ID", Type: reflect.TypeOf(0), Tag: `groupable:"True"`}
	if _, err := ParseField(field); err == nil {
		t.Fatal("expected non-canonical groupable boolean error")
	}
}
