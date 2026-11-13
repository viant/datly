package tool

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	bodyprovider "github.com/viant/bindly/provider/body"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	"github.com/viant/x/shape"
)

type strictRepresentationRecord struct {
	Name   string                       `json:"name"`
	Secret string                       `json:"secret"`
	Has    *struct{ Name, Secret bool } `json:"-" setMarker:"true"`
}
type strictRepresentationInput struct {
	Body *strictRepresentationRecord `json:"body"`
}

func TestStrictRepresentationPreservesNativeUnknownPolicy(t *testing.T) {
	canonical := testRouteContract(t, reflect.TypeFor[strictRepresentationInput](), []bindly.BindingSpec{{Path: "Body", Location: state.Location{Kind: "body"}}})
	walker, err := shape.CompileRewrite(canonical.Type())
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, field *shape.FieldMetadata) (*shape.FieldMetadata, error) {
		if parent == reflect.TypeFor[strictRepresentationRecord]() && field.Name == "Secret" {
			return &shape.FieldMetadata{Name: field.Name, Tag: `json:"-" internal:"true"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("native_source_owner", func(t *testing.T) {
		for _, raw := range []string{`{"name":"example","secret":"ignored"}`, `{"name":"example","secret":"ignored","unexpected":true}`} {
			source, err := bodyprovider.New([]byte(raw), "application/json", nil, bodyprovider.WithRepresentation(metadata.FieldShape(0)), bodyprovider.WithUnknownFieldPolicy(structjson.ErrorOnUnknown))
			if err != nil {
				t.Fatal(err)
			}
			value, found, err := source.Value(t.Context(), reflect.TypeFor[*strictRepresentationRecord](), "")
			if strings.Contains(raw, "unexpected") {
				var invalid *bindinput.Error
				if !errors.As(err, &invalid) || !strings.Contains(invalid.Cause.Error(), `unknown field "unexpected"`) {
					t.Fatalf("unexpected native source cause: %v", err)
				}
				return
			}
			if err != nil || !found {
				t.Fatalf("found=%v err=%v", found, err)
			}
			actual := value.(*strictRepresentationRecord)
			if actual.Name != "example" || actual.Secret != "" || actual.Has == nil || !actual.Has.Name || actual.Has.Secret {
				t.Fatalf("native representation admission differs: %+v", actual)
			}
		}
	})
	for _, changed := range []bool{false, true} {
		for _, strict := range []bool{false, true} {
			t.Run(map[bool]string{false: "canonical", true: "representation"}[changed]+"/"+map[bool]string{false: "lenient", true: "strict"}[strict], func(t *testing.T) {
				execution := canonical
				fields := canonical.Fields()
				if changed {
					execution, err = canonical.WithInputRepresentation(metadata)
					if err != nil {
						t.Fatal(err)
					}
					fields, err = canonical.MetadataRepresentation(metadata, nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				plan, err := NewCompiler().Compile(Input{Contract: canonical, Fields: fields, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "policy.probe", StrictArguments: &strict}})
				if err != nil {
					t.Fatal(err)
				}
				arguments := map[string]any{"body": map[string]any{"name": "example", "secret": "ignored when hidden", "unexpected": true}}
				providers, err := plan.Scope(arguments)
				if err != nil {
					t.Fatal(err)
				}
				defer providers.Close()
				injector, _ := bindly.NewInjector()
				scope, err := injector.ForScope(providers.Providers()...)
				if err != nil {
					t.Fatal(err)
				}
				var actual strictRepresentationInput
				err = scope.Bind(t.Context(), &actual, bindly.WithPlan(execution.Plan()))
				if strict {
					if err == nil {
						t.Fatalf("strict arbitrary unknown field accepted: %+v", actual.Body)
					}
					var invalid *bindinput.Error
					if !errors.As(err, &invalid) || !strings.Contains(invalid.Cause.Error(), `unknown field "unexpected"`) {
						t.Fatalf("unexpected native strict cause: %v", err)
					}
					t.Logf("strict native cause: %v", invalid.Cause)
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if actual.Body == nil || actual.Body.Name != "example" || actual.Body.Has == nil || !actual.Body.Has.Name {
					t.Fatalf("native value/presence lost: %+v", actual.Body)
				}
				if changed && (actual.Body.Secret != "" || actual.Body.Has.Secret) {
					t.Fatalf("known-hidden field admitted: %+v", actual.Body)
				}
			})
		}
	}
}
