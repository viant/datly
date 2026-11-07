package registry

import (
	"github.com/viant/tagly/choice"
	"reflect"
	"testing"
)

func TestOpenScalarInputSchema(t *testing.T) {
	p, err := choice.ParseEnum(`choice:"1:A" enumAccept:"scalar" enumInteger:"passthrough" enumUnknownString:"0"`, reflect.TypeFor[int8]())
	if err != nil {
		t.Fatal(err)
	}
	schema := EnumInputSchema(p, true)
	if !schema.Nullable || len(schema.AnyOf) != 2 {
		t.Fatalf("schema=%+v", schema)
	}
	if schema.AnyOf[0].Type != "integer" || len(schema.AnyOf[0].Enum) != 0 || schema.AnyOf[1].Type != "string" || schema.AnyOf[1].Pattern != "" || len(schema.AnyOf[1].Enum) != 0 {
		t.Fatalf("open schema narrowed: %+v", schema.AnyOf)
	}
}

func TestOpenScalarStorageBoundsAndClone(t *testing.T) {
	p, err := choice.ParseEnum(`choice:"1:A" enumAccept:"scalar" enumInteger:"passthrough"`, reflect.TypeFor[int8]())
	if err != nil {
		t.Fatal(err)
	}
	schema := EnumInputSchema(p, false)
	integer := schema.AnyOf[0]
	if integer.Minimum == nil || *integer.Minimum != -128 || integer.Maximum == nil || *integer.Maximum != 127 {
		t.Fatalf("bounds=%+v", integer)
	}
	cloned := schema.Clone()
	*cloned.AnyOf[0].Minimum = 0
	*cloned.AnyOf[0].Maximum = 0
	if *integer.Minimum != -128 || *integer.Maximum != 127 {
		t.Fatal("clone aliases storage bounds")
	}
}
