package compiler

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	xshape "github.com/viant/x/shape"
)

var configuredDecoderCalls atomic.Int64

type standardDecoder struct{}

func (*standardDecoder) Unmarshal(raw []byte, target any) error {
	configuredDecoderCalls.Add(1)
	return json.Unmarshal(raw, target)
}

type structologyDecoder struct{}

func (*structologyDecoder) Decode(raw []byte, target any) error {
	configuredDecoderCalls.Add(1)
	return structjson.Unmarshal(raw, target)
}

type configuredBody struct {
	Secret string
	Value  int
	Has    *configuredPresence `setMarker:"true" json:"-"`
}
type configuredPresence struct{ Secret, Value bool }
type configuredInput struct {
	Body *configuredBody `parameter:",kind=body,required=true"`
}

func TestEffectiveInputRetainsConfiguredJSONDecoder(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[standardDecoder](), reflect.TypeFor[structologyDecoder]()} {
		component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/configured"}}, Settings: &spec.Settings{JSONUnmarshalType: "configured"}}
		compiled, err := New(Input{Component: component, InputType: reflect.TypeFor[configuredInput](), TypeLookup: func(string) (reflect.Type, error) { return typ, nil }}).Compile()
		if err != nil {
			t.Fatal(err)
		}
		canonical, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/configured"})
		walk, _ := xshape.CompileRewrite(canonical.Type())
		effective, err := walk.Rewrite(func(_ string, parent reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
			if parent == reflect.TypeFor[configuredBody]() && f.Name == "Secret" {
				return &xshape.FieldMetadata{Name: f.Name, Tag: `json:"-" internal:"true"`}, nil
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		projected, err := canonical.WithEffectiveInput(effective)
		if err != nil {
			t.Fatal(err)
		}
		for _, changed := range []bool{false, true} {
			plan := canonical
			if changed {
				plan = projected
			}
			req := httptest.NewRequest("POST", "/configured", strings.NewReader(`{"Secret":"client","Value":0}`))
			req.Header.Set("Content-Type", "application/json")
			source, err := request.New(req)
			if err != nil {
				t.Fatal(err)
			}
			root, _ := bindly.NewInjector()
			scope, err := root.ForScope(source.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			before := configuredDecoderCalls.Load()
			var actual configuredInput
			err = scope.Bind(context.Background(), &actual, bindly.WithPlan(plan.Plan()))
			source.Close()
			if err != nil {
				t.Fatal(err)
			}
			if configuredDecoderCalls.Load() != before+1 {
				t.Fatal("selected decoder not used")
			}
			if actual.Body == nil || actual.Body.Has == nil || !actual.Body.Has.Value {
				t.Fatal("presence lost")
			}
			if changed && (actual.Body.Secret != "" || actual.Body.Has.Secret) {
				t.Fatal("effective decoder admitted hidden field")
			}
			if !changed && (actual.Body.Secret != "client" || !actual.Body.Has.Secret) {
				t.Fatal("configured base decoder changed")
			}
		}
	}
}

type configuredNullInput struct {
	Body *configuredBody `parameter:",kind=body,required=true,bodyNullPolicy=empty-record"`
}

func TestConfiguredDecoderRetainsNativeNullPolicy(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[standardDecoder](), reflect.TypeFor[structologyDecoder]()} {
		component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/null-decoder"}}, Settings: &spec.Settings{JSONUnmarshalType: "configured"}}
		compiled, err := New(Input{Component: component, InputType: reflect.TypeFor[configuredNullInput](), TypeLookup: func(string) (reflect.Type, error) { return typ, nil }}).Compile()
		if err != nil {
			t.Fatal(err)
		}
		plan, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/null-decoder"})
		for _, raw := range []string{"", "null", "{}", `{"Value":0}`} {
			req := httptest.NewRequest("POST", "/null-decoder", strings.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			source, err := request.New(req)
			if err != nil {
				t.Fatal(err)
			}
			root, _ := bindly.NewInjector()
			scope, err := root.ForScope(source.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			var actual configuredNullInput
			before := configuredDecoderCalls.Load()
			err = scope.Bind(t.Context(), &actual, bindly.WithPlan(plan.Plan()))
			source.Close()
			if raw == "" {
				if err == nil {
					t.Fatal("missing body accepted")
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			calls := int64(1)
			if raw == "null" {
				calls = 0
			}
			if configuredDecoderCalls.Load() != before+calls {
				t.Fatal("incorrect decoder invocation")
			}
			if actual.Body == nil || actual.Body.Has == nil {
				t.Fatal("native empty-record presence lost")
			}
			if actual.Body.Has.Value != (raw != "null" && raw != "{}") {
				t.Fatal("incorrect nested presence")
			}
		}
	}
}

type ConfiguredMetadataMethods struct{}

func (*ConfiguredMetadataMethods) Linked() bool { return true }

type configuredMetadataBody struct {
	*ConfiguredMetadataMethods
	Rows [][]*configuredBody `json:"rows"`
}
type configuredMetadataInput struct {
	Body *configuredMetadataBody `parameter:",kind=body,required=true,bodyNullPolicy=empty-record"`
}
type metadataStandardDecoder struct{ standardDecoder }

func (*metadataStandardDecoder) UnmarshalWithRepresentation(raw []byte, target any, metadata *xshape.Representation) error {
	configuredDecoderCalls.Add(1)
	return structjson.UnmarshalStandard(raw, target, structjson.WithRepresentation(metadata))
}

type metadataStructologyDecoder struct{ structologyDecoder }

func (*metadataStructologyDecoder) UnmarshalWithRepresentation(raw []byte, target any, metadata *xshape.Representation) error {
	configuredDecoderCalls.Add(1)
	return structjson.Unmarshal(raw, target, structjson.WithRepresentation(metadata))
}
func TestConfiguredDecoderMetadataKeepsEmbeddedInstances(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[metadataStandardDecoder](), reflect.TypeFor[metadataStructologyDecoder]()} {
		component := &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/configured-metadata"}}, Settings: &spec.Settings{JSONUnmarshalType: "configured"}}
		compiled, err := New(Input{Component: component, InputType: reflect.TypeFor[configuredMetadataInput](), TypeLookup: func(string) (reflect.Type, error) { return typ, nil }}).Compile()
		if err != nil {
			t.Fatal(err)
		}
		canonical, _ := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/configured-metadata"})
		walker, _ := xshape.CompileRewrite(canonical.Type())
		metadata, err := walker.RewriteRepresentation(func(_ string, parent reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
			if parent == reflect.TypeFor[configuredBody]() && f.Name == "Secret" {
				return &xshape.FieldMetadata{Name: f.Name, Tag: `json:"-" internal:"true"`}, nil
			}
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		plan, err := canonical.WithInputRepresentation(metadata)
		if err != nil {
			t.Fatal(err)
		}
		for _, raw := range []string{`{"rows":[[{"Secret":"blocked","Value":0},null]]}`, `null`} {
			req := httptest.NewRequest("POST", "/configured-metadata", strings.NewReader(raw))
			req.Header.Set("Content-Type", "application/json")
			source, err := request.New(req)
			if err != nil {
				t.Fatal(err)
			}
			root, _ := bindly.NewInjector()
			scope, err := root.ForScope(source.Providers()...)
			if err != nil {
				t.Fatal(err)
			}
			before := configuredDecoderCalls.Load()
			var actual configuredMetadataInput
			err = scope.Bind(t.Context(), &actual, bindly.WithPlan(plan.Plan()))
			source.Close()
			if err != nil {
				t.Fatal(err)
			}
			if actual.Body == nil || !actual.Body.Linked() {
				t.Fatal("linked body lost")
			}
			expectedCalls := int64(1)
			if raw == "null" {
				expectedCalls = 0
			} else {
				leaf := actual.Body.Rows[0][0]
				if leaf.Secret != "" || leaf.Has == nil || !leaf.Has.Value || leaf.Has.Secret || actual.Body.Rows[0][1] != nil {
					t.Fatal("effective native decoding/presence lost")
				}
			}
			if configuredDecoderCalls.Load() != before+expectedCalls {
				t.Fatal("configured decoder ownership lost")
			}
		}
	}
}
