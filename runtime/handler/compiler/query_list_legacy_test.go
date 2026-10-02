package compiler

import (
	"context"
	"reflect"
	"strconv"
	"testing"
)

// Expectations follow Platform's pinned Datly 69de0137b444 query locator,
// session adjustValue and internal/converter. Mixed CSV/repeated merging is
// the native extension, tested separately from original single/repeated parity.
func TestOriginalDatlyQueryListWireCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     any
		want    any
		invalid bool
	}{
		{"empty numeric", "", []int{}, false},
		{"holes numeric", "1,,2,", []int{1, 2}, false},
		{"trim numeric", " 1 , 2 ", []int{1, 2}, false},
		{"brackets numeric", "[1,2]", []int{1, 2}, false},
		{"bracket whitespace", " [1,2] ", []int(nil), true},
		{"scientific fractions", "1e2,2.9,-2.9", []int{100, 2, -2}, false},
		{"repeated integers", []string{"1", "2", "1"}, []int{1, 2, 1}, false},
		{"repeated empty rejected", []string{"", "1"}, []int(nil), true},
		{"repeated spaces rejected", []string{" 1 ", "2"}, []int(nil), true},
		{"mixed extension", []string{"3,,1", "2", "[1,4]"}, []int{3, 1, 2, 1, 4}, false},
		{"empty strings", "", []string{}, false},
		{"string holes", "a,,b,", []string{"a", "", "b", ""}, false},
		{"string whitespace", " a , b ", []string{" a ", " b "}, false},
		{"empty enclosure string", "[]", []string{""}, false},
		{"repeated string empty", []string{"", "a"}, []string{"", "a"}, false},
		{"string merging extension", []string{"a,b", "c"}, []string{"a", "b", "c"}, false},
		{"bool normalization", " true, ,false ", []bool{true, false}, false},
		{"float normalization", " 1.5,,2.5 ", []float64{1.5, 2.5}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := (&queryListTransformer{target: reflect.TypeOf(tc.want)}).Transform(context.Background(), nil, tc.raw)
			if tc.invalid {
				if err == nil {
					t.Fatalf("invalid original wire accepted: %v", got)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v: %v", got, tc.want, err)
			}
		})
	}
}

func TestOriginalDatlyQueryIntegerCastCompatibility(t *testing.T) {
	type identifier int64
	for _, raw := range []string{"-1", "2.9", "1e2", "9007199254740993", "999999999999999999999999", "NaN", "+Inf", "-Inf"} {
		floating, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatal(err)
		}
		// The pinned original casts through a machine int. Extreme and
		// non-finite casts must be compared on the same architecture.
		original := int(floating)
		for _, want := range []any{[]int{original}, []int64{int64(original)}, []uint{uint(original)}, []uint64{uint64(uint(original))}, []identifier{identifier(original)}} {
			got, err := (&queryListTransformer{target: reflect.TypeOf(want)}).Transform(context.Background(), nil, raw)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s -> %T: %#v want %#v: %v", raw, want, got, want, err)
			}
		}
	}
	if _, err := (&queryListTransformer{target: reflect.TypeFor[[]int8]()}).Transform(context.Background(), nil, "128"); err == nil {
		t.Fatal("small integer range check lost")
	}
}
