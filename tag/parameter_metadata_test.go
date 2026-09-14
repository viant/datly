package tag

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestParseFieldParameterMetadata(t *testing.T) {
	field := reflect.StructField{
		Name: "Fields",
		Type: reflect.TypeOf([]string{}),
		Tag:  `bind:"Fields,kind=query,in=fields" querySelector:"view=users" predicate:"contains,group=2,u,name" predicate:"tenant,applyWhenAbsent=true,tenant_id=7" desc:"selected fields" example:"id,name"`,
	}
	metadata, err := ParseField(field)
	if err != nil {
		t.Fatalf("ParseField() error = %v", err)
	}
	if metadata.QuerySelector == nil || metadata.QuerySelector.View != "users" || metadata.Description != "selected fields" || metadata.Example != "id,name" {
		t.Fatalf("unexpected field metadata: %+v", metadata)
	}
	if len(metadata.Predicates) != 2 || metadata.Predicates[0].Name != "contains" || metadata.Predicates[0].Group != 2 ||
		len(metadata.Predicates[0].Args) != 2 || metadata.Predicates[1].Name != "tenant" || !metadata.Predicates[1].ApplyWhenAbsent ||
		len(metadata.Predicates[1].Args) != 1 || metadata.Predicates[1].Args[0] != "tenant_id=7" {
		t.Fatalf("unexpected predicates: %+v", metadata.Predicates)
	}
}

func TestPredicateValueRoundTripsCanonicalMetadata(t *testing.T) {
	source := &spec.Predicate{
		Group: 2, Name: "contains", ApplyWhenAbsent: true,
		Args: []string{"u", "name,last", "tenant=id", "group=raw", "applyWhenAbsent=raw"},
	}
	value, err := PredicateValue(source)
	if err != nil {
		t.Fatalf("PredicateValue() error = %v", err)
	}
	field := reflect.StructField{Name: "Value", Type: reflect.TypeOf(""), Tag: reflect.StructTag(`predicate:"` + value + `"`)}
	actual, err := ParsePredicates(field.Tag)
	if err != nil {
		t.Fatalf("ParsePredicates() error = %v\n%s", err, value)
	}
	if len(actual) != 1 || actual[0].Name != source.Name || actual[0].Group != source.Group || !actual[0].ApplyWhenAbsent ||
		len(actual[0].Args) != len(source.Args) {
		t.Fatalf("round trip = %+v\n%s", actual, value)
	}
	for index := range source.Args {
		if actual[0].Args[index] != source.Args[index] {
			t.Fatalf("argument %d = %q, want %q\n%s", index, actual[0].Args[index], source.Args[index], value)
		}
	}
}

func TestParseFieldRejectsMalformedParameterMetadata(t *testing.T) {
	tests := []reflect.StructTag{
		`predicate:"contains,group=nope"`,
		`predicate:"contains,applyWhenAbsent=maybe"`,
		`predicate:"contains,ensure=true"`,
		`predicate:",group=1"`,
		`querySelector:"other=users"`,
		`querySelector:"view="`,
	}
	for _, structTag := range tests {
		t.Run(string(structTag), func(t *testing.T) {
			if _, err := ParseField(reflect.StructField{Name: "Fields", Type: reflect.TypeOf([]string{}), Tag: structTag}); err == nil {
				t.Fatalf("ParseField(%q) expected error", structTag)
			}
		})
	}
}
