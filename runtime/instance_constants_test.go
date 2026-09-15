package runtime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/constant"
	"github.com/viant/datly/internal/testharness"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
)

func TestInstanceFilesTypedBindingPreservesDefaults(t *testing.T) {
	type input struct {
		Number   int64
		Unsigned uint64
		Zero     int
		Flag     bool
		Empty    string
		Default  int
	}
	type output = input
	defaults := map[string]string{"Number": "7", "Unsigned": "8", "Zero": "9", "Flag": "true", "Empty": "authored", "Default": "42"}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example/app", Name: "Constants"}, Name: "Constants", Routes: []*spec.Route{{Method: "GET", Path: "/constants"}}}
	for _, field := range reflect.VisibleFields(reflect.TypeFor[input]()) {
		value := defaults[field.Name]
		component.Parameters = append(component.Parameters, &spec.Parameter{Name: field.Name, Source: spec.BindSource{Kind: "const", Name: field.Name}, TypeExpr: field.Type.String(), Value: &value})
	}
	before, _ := json.Marshal(component)
	for _, format := range []string{"yaml", "json"} {
		t.Run(format, func(t *testing.T) {
			text := `{"Number":9007199254740993,"Unsigned":18446744073709551615,"Zero":0,"Flag":false,"Empty":""}`
			if format == "yaml" {
				text = "Number: 9007199254740993\nUnsigned: 18446744073709551615\nZero: 0\nFlag: false\nEmpty: ''\n"
			}
			file := filepath.Join(t.TempDir(), "const."+format)
			if err := os.WriteFile(file, []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			values, err := (constant.Loader{}).Load(context.Background(), file)
			if err != nil {
				t.Fatal(err)
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Const: values, Component: component, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[output]()})
			if err != nil {
				t.Fatal(err)
			}
			runtime, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[output](), Handler: customhandler.NewFunc[input, output](func(_ context.Context, value *input) (*output, error) { return value, nil })}})
			if err != nil {
				t.Fatal(err)
			}
			actual, err := executeTestRoute(t, runtime, context.Background(), testharness.NewRequest("GET", "/constants?Number=1&Flag=true&Default=0"))
			if err != nil {
				t.Fatal(err)
			}
			want := output{Number: 9007199254740993, Unsigned: 18446744073709551615, Zero: 0, Flag: false, Empty: "", Default: 42}
			if got := actual.(*output); *got != want {
				t.Fatalf("got=%+v want=%+v", got, want)
			}
		})
	}
	after, _ := json.Marshal(component)
	if string(after) != string(before) {
		t.Fatal("shared authored constants mutated")
	}
}
