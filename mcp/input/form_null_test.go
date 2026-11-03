package input

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
)

func TestOptionalFormBooleanNullUsesNativeAbsence(t *testing.T) {
	type selection struct{ Optional *bool }
	for _, tc := range []struct {
		name string
		args Arguments
		want *bool
	}{
		{name: "omitted", args: Arguments{}},
		{name: "cleared", args: Arguments{"Optional": nil}},
		{name: "false", args: Arguments{"Optional": false}, want: boolPointer(false)},
		{name: "true", args: Arguments{"Optional": true}, want: boolPointer(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			location := bindstate.Location{Kind: "form", In: "optional"}
			plan, err := NewCompiler().Compile([]Argument{{PublicName: "Optional", Source: location, SourceType: reflect.TypeFor[*bool]()}})
			if err != nil {
				t.Fatal(err)
			}
			scope, err := plan.Scope(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			injector, err := bindly.NewInjector(bindly.WithProviders(scope.Providers()...))
			if err != nil {
				t.Fatal(err)
			}
			required := false
			binding, err := injector.CompilePlan(reflect.TypeFor[*selection](), bindly.BindingSpec{Path: "Optional", Location: location, SourceType: reflect.TypeFor[*bool](), Required: &required})
			if err != nil {
				t.Fatal(err)
			}
			var actual selection
			if err = injector.Bind(context.Background(), &actual, bindly.WithPlan(binding)); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual.Optional, tc.want) {
				t.Fatalf("optional=%v want %v", actual.Optional, tc.want)
			}
		})
	}
}
func boolPointer(value bool) *bool { return &value }
