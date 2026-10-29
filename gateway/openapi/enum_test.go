package openapi

import (
	"github.com/viant/datly/gateway/openapi/openapi3"
	"testing"
)

func TestEnumSchemaCopiesNestedReferencedMember(t *testing.T) {
	original := &openapi3.Schema{Type: "object", Properties: openapi3.Schemas{"mode": {Type: "integer"}}}
	builder := newSchemaBuilder(openapi3.Schemas{"Filter": original})
	root := &openapi3.Schema{Ref: "#/components/schemas/Filter"}
	values := []any{int64(1), int64(2)}
	actual, ok := builder.enumSchema(root, []string{"mode"}, values, 0)
	if !ok || len(actual.Properties["mode"].Enum) != 2 {
		t.Fatal("enum path omitted")
	}
	values[0] = int64(9)
	if len(original.Properties["mode"].Enum) != 0 || actual.Properties["mode"].Enum[0] != int64(1) {
		t.Fatal("shared enum schema mutated")
	}
}

func TestNullableEnumIncludesNullWithoutMutatingSource(t *testing.T) {
	source := &openapi3.Schema{Type: "object", Properties: openapi3.Schemas{"mode": {Type: "integer", Nullable: true}}}
	builder := newSchemaBuilder(openapi3.Schemas{})
	values := []any{int64(9007199254740993)}
	actual, ok := builder.enumSchema(source, []string{"mode"}, values, 0)
	if !ok || !actual.Properties["mode"].Nullable || len(actual.Properties["mode"].Enum) != 2 || actual.Properties["mode"].Enum[1] != nil {
		t.Fatalf("nullable enum omits explicit null: %+v", actual)
	}
	if len(source.Properties["mode"].Enum) != 0 || len(values) != 1 {
		t.Fatal("source enum mutated")
	}
	again, ok := builder.enumSchema(actual, []string{"mode"}, actual.Properties["mode"].Enum, 0)
	if !ok || len(again.Properties["mode"].Enum) != 2 {
		t.Fatal("null duplicated")
	}
	strict, ok := builder.enumSchema(&openapi3.Schema{Type: "integer"}, nil, values, 0)
	if !ok || len(strict.Enum) != 1 {
		t.Fatal("non-nullable scalar admitted null")
	}
}

func TestEnumSchemaSelectsExactCaseBeforeFallback(t *testing.T) {
	source := &openapi3.Schema{Type: "object", Properties: openapi3.Schemas{"Mode": {Type: "integer"}, "mode": {Type: "integer"}}}
	builder := newSchemaBuilder(openapi3.Schemas{})
	actual, ok := builder.enumSchema(source, []string{"mode"}, []any{1}, 0)
	if !ok || len(actual.Properties["mode"].Enum) != 1 || len(actual.Properties["Mode"].Enum) != 0 {
		t.Fatal("enum attached to wrong case-sensitive property")
	}
	if _, ok = builder.enumSchema(source, []string{"MODE"}, []any{1}, 0); ok {
		t.Fatal("ambiguous case fallback accepted")
	}
}
