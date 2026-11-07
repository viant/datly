package registry_test

import (
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	structjson "github.com/viant/structology/encoding/json"
	xshape "github.com/viant/x/shape"
	"github.com/viant/xdatly/contract"
)

type linkedCustomChild struct {
	Value    string
	Presence bool
}

func (c *linkedCustomChild) UnmarshalJSON(raw []byte) error {
	var wire struct {
		Value string `json:"wire_value"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	c.Value = "normalized:" + wire.Value
	c.Presence = true
	return nil
}

type linkedCompositeBody struct {
	Spend float64
	Child *linkedCustomChild
}
type linkedCompositeInput struct {
	Body *linkedCompositeBody `parameter:",kind=body,required=true"`
}

func TestEffectiveWrapperRetainsLinkedCustomChildDecoder(t *testing.T) {
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/composite"}}}, InputType: reflect.TypeFor[linkedCompositeInput](), OutputType: reflect.TypeFor[struct{}]()})
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := artifact.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/composite"})
	walk, _ := xshape.CompileRewrite(canonical.Type())
	effective, err := walk.Rewrite(func(_ string, parent reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if parent == reflect.TypeFor[linkedCompositeBody]() && f.Name == "Spend" {
			return &xshape.FieldMetadata{Name: f.Name, Tag: `json:"-" internal:"true"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	bodyField, _ := effective.FieldByName("Body")
	childField, _ := bodyField.Type.Elem().FieldByName("Child")
	if childField.Type != reflect.TypeFor[*linkedCustomChild]() {
		t.Fatal("custom child lost original linked type")
	}
	// Structology's native decoder must also dispatch to the unchanged linked
	// child method when its enclosing representation is a rewritten struct.
	decoded := reflect.New(bodyField.Type.Elem())
	if err = structjson.Unmarshal([]byte(`{"Spend":42,"Child":{"wire_value":"structology"}}`), decoded.Interface()); err != nil {
		t.Fatal(err)
	}
	custom := decoded.Elem().FieldByName("Child").Interface().(*linkedCustomChild)
	if custom.Value != "normalized:structology" || !custom.Presence {
		t.Fatal("Structology bypassed custom child decoder")
	}
	if decoded.Elem().FieldByName("Spend").Float() != 0 {
		t.Fatal("Structology decoded hidden wrapper field")
	}
	plan, err := canonical.WithEffectiveInput(effective)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/composite", strings.NewReader(`{"Spend":42,"Child":{"wire_value":"accepted"}}`))
	req.Header.Set("Content-Type", "application/json")
	source, err := request.New(req)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	root, _ := bindly.NewInjector()
	scope, err := root.ForScope(source.Providers()...)
	if err != nil {
		t.Fatal(err)
	}
	var value linkedCompositeInput
	if err = scope.Bind(t.Context(), &value, bindly.WithPlan(plan.Plan())); err != nil {
		t.Fatal(err)
	}
	if value.Body == nil || value.Body.Spend != 0 || value.Body.Child == nil || value.Body.Child.Value != "normalized:accepted" || !value.Body.Child.Presence {
		t.Fatalf("custom decode or hidden wrapper field failed: %+v", value.Body)
	}
}

type effectiveBody struct {
	Secret string
	Value  int
	Has    *effectiveBodyHas `setMarker:"true" json:"-"`
}
type effectiveBodyHas struct{ Secret, Value bool }
type effectiveInput struct {
	Secret string         `parameter:",kind=query,in=secret"`
	Reason string         `parameter:",kind=query,in=reason"`
	Body   *effectiveBody `parameter:",kind=body,required=true"`
}

func TestEffectiveInputNativeDecodePreservesCanonicalTypeAndPresence(t *testing.T) {
	component, err := (bootstrap.ContractResolver{Component: &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/effective"}}}, InputType: xshape.Linked(reflect.TypeFor[effectiveInput]()).Descriptor()}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[effectiveInput](), OutputType: reflect.TypeFor[struct{}]()})
	if err != nil {
		t.Fatal(err)
	}
	original, _ := artifact.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/effective"})
	walk, _ := xshape.CompileRewrite(original.Type())
	effective, err := walk.Rewrite(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		switch f.Name {
		case "Secret":
			return &xshape.FieldMetadata{Name: f.Name, Tag: reflect.StructTag(string(f.Tag) + ` json:"-" internal:"true"`)}, nil
		case "Reason":
			return &xshape.FieldMetadata{Name: f.Name, Tag: reflect.StructTag(string(f.Tag) + ` required:"true"`)}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := original.WithEffectiveInput(effective)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"?secret=attack&reason=review", "?secret=attack"} {
		req := httptest.NewRequest("POST", "/effective"+query, strings.NewReader(`{"Secret":"attack","Value":0}`))
		req.Header.Set("Content-Type", "application/json")
		source, err := request.New(req)
		if err != nil {
			t.Fatal(err)
		}
		defer source.Close()
		root, _ := bindly.NewInjector()
		scope, err := root.ForScope(source.Providers()...)
		if err != nil {
			t.Fatal(err)
		}
		var actual effectiveInput
		err = scope.Bind(t.Context(), &actual, bindly.WithPlan(plan.Plan()))
		if !strings.Contains(query, "reason=") {
			if err == nil {
				t.Fatal("required field not enforced")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if actual.Secret != "" || actual.Body == nil || actual.Body.Secret != "" || actual.Body.Has == nil || actual.Body.Has.Secret || !actual.Body.Has.Value || actual.Body.Value != 0 {
			t.Fatalf("canonical presence %+v body %+v", actual, actual.Body)
		}
	}
	if plan.Type() != original.Type() {
		t.Fatal("handler input identity changed")
	}
	if len(original.Fields()) != 3 {
		t.Fatal("canonical plan mutated")
	}
}

func TestEffectiveNestedRequiredUsesNativeBodyPresence(t *testing.T) {
	type row struct {
		Value int
		Flag  bool
	}
	type body struct{ Rows []*row }
	type input struct {
		Body *body `parameter:",kind=body,required=true"`
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/required"}}}, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[struct{}]()})
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := artifact.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/required"})
	walk, _ := xshape.CompileRewrite(canonical.Type())
	effective, err := walk.Rewrite(func(_ string, parent reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if parent == reflect.TypeFor[row]() {
			return &xshape.FieldMetadata{Name: f.Name, Tag: `required:"true"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := canonical.WithEffectiveInput(effective)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		raw   string
		valid bool
	}{{`{"Rows":[{"Value":0,"Flag":false}]}`, true}, {`{"Rows":[{"Value":null,"Flag":false}]}`, true}, {`{"Rows":[{"Flag":false}]}`, false}, {`{"Rows":[{}]}`, false}, {`{"Rows":[]}`, true}} {
		req := httptest.NewRequest("POST", "/required", strings.NewReader(tc.raw))
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
		var target input
		err = scope.Bind(t.Context(), &target, bindly.WithPlan(plan.Plan()))
		source.Close()
		if (err == nil) != tc.valid {
			t.Fatalf("%s: %v", tc.raw, err)
		}
	}
}

func TestEffectiveOptionalRemovesNativeBindingRequirement(t *testing.T) {
	type input struct {
		Value string `parameter:",kind=query,in=value,required=true" required:"true"`
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/optional"}}}, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[struct{}]()})
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := artifact.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/optional"})
	walk, _ := xshape.CompileRewrite(canonical.Type())
	effective, err := walk.Rewrite(func(_ string, _ reflect.Type, field *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		f := (&contract.Field{Name: field.Name, Tag: field.Tag}).Optional()
		return &xshape.FieldMetadata{Name: f.Name, Tag: f.Tag}, f.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := canonical.WithEffectiveInput(effective)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("GET", "/optional", nil)
	source, err := request.New(req)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	root, _ := bindly.NewInjector()
	scope, err := root.ForScope(source.Providers()...)
	if err != nil {
		t.Fatal(err)
	}
	var value input
	if err = scope.Bind(t.Context(), &value, bindly.WithPlan(plan.Plan())); err != nil {
		t.Fatal(err)
	}
	if err = scope.Bind(t.Context(), &value, bindly.WithPlan(canonical.Plan())); err == nil {
		t.Fatal("canonical requiredness was lost")
	}
}

func TestEffectiveBodyPreservesEmptyRecordNullPolicy(t *testing.T) {
	type row struct {
		Secret string
		Value  int
		Has    *struct{ Secret, Value bool } `setMarker:"true" json:"-"`
	}
	type input struct {
		Body *row                 `parameter:",kind=body,required=true,bodyNullPolicy=empty-record"`
		Has  *struct{ Body bool } `setMarker:"true" json:"-"`
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/null-effective"}}}, InputType: reflect.TypeFor[input](), OutputType: reflect.TypeFor[struct{}]()})
	if err != nil {
		t.Fatal(err)
	}
	canonical, _ := artifact.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/null-effective"})
	walker, _ := xshape.CompileRewrite(canonical.Type())
	effective, err := walker.Rewrite(func(_ string, parent reflect.Type, field *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if parent == reflect.TypeFor[row]() && field.Name == "Secret" {
			return &xshape.FieldMetadata{Name: field.Name, Tag: `json:"-" internal:"true"`}, nil
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := canonical.WithEffectiveInput(effective)
	if err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{"", "null", "{}", `{"Value":0,"Secret":"blocked"}`} {
		req := httptest.NewRequest("POST", "/null-effective", strings.NewReader(raw))
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
		var actual input
		err = scope.Bind(t.Context(), &actual, bindly.WithPlan(plan.Plan()))
		source.Close()
		if raw == "" {
			if err == nil {
				t.Fatal("missing required body accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if actual.Body == nil || actual.Body.Has == nil || actual.Has == nil || !actual.Has.Body {
			t.Fatalf("lost null/body presence: %s %+v", raw, actual)
		}
		if actual.Body.Secret != "" || actual.Body.Has.Secret {
			t.Fatal("hidden field was admitted")
		}
		if actual.Body.Has.Value != (raw != "null" && raw != "{}") {
			t.Fatalf("wrong nested presence for %s", raw)
		}
	}
}
