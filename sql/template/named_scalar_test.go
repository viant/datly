package template

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type namedLabel string
type namedCount int
type namedEpoch int64
type namedRatio float64
type namedWeight float32
type namedEnabled bool

func TestCompilerNamedScalarState(t *testing.T) {
	type input struct {
		Label   namedLabel
		Count   namedCount
		Epoch   namedEpoch
		Ratio   namedRatio
		Weight  namedWeight
		Enabled namedEnabled
	}
	program, err := (Compiler{Source: `#if($Enabled) SELECT $Count WHERE label = $Label #else SELECT 0 #end`, InputType: reflect.TypeFor[input](), Variables: []Variable{{Name: "Bound", Type: reflect.TypeFor[namedLabel](), Key: "bound"}}}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []bool{true, false, true} {
		in := input{Label: "ready", Count: 7, Epoch: 123, Ratio: 1.5, Weight: 2.5, Enabled: namedEnabled(enabled)}
		result, err := program.Evaluate(context.Background(), Invocation{Input: reflect.ValueOf(in), Binder: templateBinder{"bound": namedLabel("fresh")}})
		if err != nil {
			t.Fatal(err)
		}
		want := "SELECT :Count WHERE label = :Label"
		if !enabled {
			want = "SELECT 0"
		}
		if strings.TrimSpace(result.SQL) != want {
			t.Fatalf("SQL %q want %q", result.SQL, want)
		}
	}
}

func TestVariableValueNamedScalarBoundary(t *testing.T) {
	for _, tc := range []struct{ in, want any }{{namedLabel("x"), "x"}, {namedCount(3), int(3)}, {namedEpoch(4), int64(4)}, {namedRatio(1.5), float64(1.5)}, {namedWeight(2.5), float32(2.5)}, {namedEnabled(true), true}} {
		got := variableValue(reflect.ValueOf(tc.in))
		if !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("%T boundary value=%T(%v)", tc.in, got, got)
		}
	}
}
