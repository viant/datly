package input

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
)

func TestQueryArgumentsPreserveNativeDefaults(t *testing.T) {
	type input struct{ Source string }
	for _, tc := range []struct {
		description string
		arguments   Arguments
		expected    string
	}{
		{"omitted", Arguments{}, "ui"},
		{"null", Arguments{"Source": nil}, "ui"},
		{"empty", Arguments{"Source": ""}, "ui"},
		{"value", Arguments{"Source": "pws"}, "pws"},
		{"whitespace", Arguments{"Source": " "}, " "},
		{"literal null", Arguments{"Source": "null"}, "null"},
	} {
		t.Run(tc.description, func(t *testing.T) {
			location := bindstate.Location{Kind: "query", In: "source"}
			plan, err := NewCompiler().Compile([]Argument{{PublicName: "Source", Source: location, SourceType: reflect.TypeFor[string]()}})
			if err != nil {
				t.Fatal(err)
			}
			scope, err := plan.Scope(tc.arguments)
			if err != nil {
				t.Fatal(err)
			}
			injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
			if err != nil {
				t.Fatal(err)
			}
			optional := false
			binding, err := injector.CompilePlan(reflect.TypeFor[*input](), bindly.BindingSpec{Path: "Source", Location: location, SourceType: reflect.TypeFor[string](), DefaultValue: "ui", Required: &optional})
			if err != nil {
				t.Fatal(err)
			}
			var actual input
			if err = injector.Bind(context.Background(), &actual, bindly.WithPlan(binding)); err != nil {
				t.Fatal(err)
			}
			if actual.Source != tc.expected {
				t.Fatalf("Source=%q want %q", actual.Source, tc.expected)
			}
		})
	}
}

func TestQueryEmptyProjectionKeepsOtherValues(t *testing.T) {
	for _, tc := range []struct {
		description, kind string
		value             any
		valueType         reflect.Type
		found             bool
		wire              []string
		body              string
	}{
		{"integer null", "query", nil, reflect.TypeFor[int](), false, nil, ""},
		{"pointer null", "query", nil, reflect.TypeFor[*string](), false, nil, ""},
		{"zero", "query", 0, reflect.TypeFor[int](), true, []string{"0"}, ""},
		{"false", "query", false, reflect.TypeFor[bool](), true, []string{"false"}, ""},
		{"empty array", "query", []string{}, reflect.TypeFor[[]string](), true, nil, ""},
		{"array empty element", "query", []string{""}, reflect.TypeFor[[]string](), true, []string{""}, ""},
		{"body null", "body", nil, reflect.TypeFor[*string](), true, nil, "null"},
		{"body empty", "body", "", reflect.TypeFor[string](), true, nil, `""`},
		{"header empty", "header", "", reflect.TypeFor[string](), true, []string{""}, ""},
		{"form empty", "form", "", reflect.TypeFor[string](), true, []string{""}, ""},
		{"path empty", "path", "", reflect.TypeFor[string](), true, []string{""}, ""},
	} {
		t.Run(tc.description, func(t *testing.T) {
			actual, found, err := (Arguments{"Value": tc.value}).resolve(Argument{PublicName: "Value", Source: bindstate.Location{Kind: tc.kind, In: "value"}, SourceType: tc.valueType})
			if err != nil {
				t.Fatal(err)
			}
			if found != tc.found || len(actual.wire) != len(tc.wire) || string(actual.body) != tc.body {
				t.Fatalf("found=%v wire=%v body=%s", found, actual.wire, actual.body)
			}
			for i := range tc.wire {
				if actual.wire[i] != tc.wire[i] {
					t.Fatalf("wire=%v want %v", actual.wire, tc.wire)
				}
			}
		})
	}
}

func TestEmptyQueryArgumentsStillFailRequiredBinding(t *testing.T) {
	type input struct{ Source string }
	for _, tc := range []struct {
		description string
		arguments   Arguments
	}{
		{"omitted", Arguments{}}, {"null", Arguments{"Source": nil}}, {"empty", Arguments{"Source": ""}},
	} {
		t.Run(tc.description, func(t *testing.T) {
			location := bindstate.Location{Kind: "query", In: "source"}
			plan, err := NewCompiler().Compile([]Argument{{PublicName: "Source", Source: location, SourceType: reflect.TypeFor[string]()}})
			if err != nil {
				t.Fatal(err)
			}
			scope, err := plan.Scope(tc.arguments)
			if err != nil {
				t.Fatal(err)
			}
			injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
			if err != nil {
				t.Fatal(err)
			}
			required := true
			binding, err := injector.CompilePlan(reflect.TypeFor[*input](), bindly.BindingSpec{Path: "Source", Location: location, SourceType: reflect.TypeFor[string](), Required: &required})
			if err != nil {
				t.Fatal(err)
			}
			var actual input
			if err = injector.Bind(context.Background(), &actual, bindly.WithPlan(binding)); err == nil {
				t.Fatal("missing required query argument was accepted")
			}
		})
	}
}
