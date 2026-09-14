package data

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestFromViewPointerNullFallback(t *testing.T) {
	for _, scalar := range []struct{ name, fallback string }{
		{"int", "0"}, {"int8", "0"}, {"int16", "0"}, {"int32", "0"}, {"int64", "0"},
		{"uint", "0"}, {"uint8", "0"}, {"uint16", "0"}, {"uint32", "0"}, {"uint64", "0"},
		{"float32", "0"}, {"float64", "0"}, {"string", "''"}, {"bool", "FALSE"},
	} {
		for _, pointer := range []bool{false, true} {
			for _, explicit := range []bool{false, true} {
				name := scalar.name
				if pointer {
					name = "*" + name
				}
				if explicit {
					name += " explicit"
				}
				t.Run(name, func(t *testing.T) {
					canonical := &spec.Column{Name: "Value", Source: "value", Type: spec.TypeRef{Name: scalar.name, Pointer: pointer}, Nullable: true, ExplicitType: explicit}
					before := canonical.Clone()
					column := FromView(nil, &spec.View{Columns: []*spec.Column{canonical}}).Columns[0]
					want := scalar.fallback
					if pointer {
						want = ""
					}
					if !column.Nullable || column.NullFallback != want {
						t.Fatalf("column = %+v, want fallback %q", column, want)
					}
					if strings.Contains(column.SelectExpression(false), "COALESCE") == pointer {
						t.Fatalf("wrong null projection: %s", column.SelectExpression(false))
					}
					if strings.Contains(column.SelectExpression(true), "COALESCE") {
						t.Fatal("AllowNulls did not suppress fallback")
					}
					if !reflect.DeepEqual(canonical, before) {
						t.Fatal("canonical pointer authority was mutated")
					}
				})
			}
		}
	}
}

func TestColumnNullFallbackPreservesUnsupportedTypes(t *testing.T) {
	for _, name := range []string{"*int", "*string", "Payload", "[]int", "map[string]int", "any"} {
		column := &Column{}
		column.ConfigureNullability(true, name)
		if !column.Nullable || column.NullFallback != "" {
			t.Fatalf("%s: %+v", name, column)
		}
	}
	column := &Column{}
	column.ConfigureNullability(true, "int")
	column.ConfigureNullability(false, "int")
	if column.Nullable || column.NullFallback != "" {
		t.Fatalf("stale fallback: %+v", column)
	}
}
