package spec

import "testing"

func TestColumnEffectiveType(t *testing.T) {
	tests := []struct {
		name   string
		column *Column
		want   TypeRef
	}{
		{name: "scalar", column: &Column{Type: TypeRef{Name: "int64"}}, want: TypeRef{Name: "int64"}},
		{name: "nullable scalar", column: &Column{Type: TypeRef{Name: "int64"}, Nullable: true}, want: TypeRef{Name: "int64", Pointer: true}},
		{name: "explicit pointer", column: &Column{Type: TypeRef{Name: "int64", Pointer: true}}, want: TypeRef{Name: "int64", Pointer: true}},
		{name: "slice", column: &Column{Type: TypeRef{Name: "string", Cardinality: CardinalityMany}, Nullable: true}, want: TypeRef{Name: "string", Cardinality: CardinalityMany}},
		{name: "cast value overrides nullable", column: &Column{Type: TypeRef{Name: "T"}, ExplicitType: true, Nullable: true}, want: TypeRef{Name: "T"}},
		{name: "cast slice elements", column: &Column{Type: TypeRef{Name: "T", Cardinality: CardinalityMany, Pointer: true}, ExplicitType: true, Nullable: true}, want: TypeRef{Name: "T", Cardinality: CardinalityMany, Pointer: true}},
		{name: "cast slice pointer", column: &Column{Type: TypeRef{Name: "T", Cardinality: CardinalityMany, SlicePointer: true}, ExplicitType: true, Nullable: true}, want: TypeRef{Name: "T", Cardinality: CardinalityMany, SlicePointer: true}},
		{name: "map", column: &Column{Type: TypeRef{Name: "map[string]string"}, Nullable: true}, want: TypeRef{Name: "map[string]string"}},
		{name: "any", column: &Column{Type: TypeRef{Name: "any"}, Nullable: true}, want: TypeRef{Name: "any"}},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if actual := testCase.column.EffectiveType(); actual != testCase.want {
				t.Fatalf("EffectiveType() = %+v, want %+v", actual, testCase.want)
			}
		})
	}
}
