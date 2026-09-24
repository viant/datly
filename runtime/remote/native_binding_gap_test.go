package remote

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator/buildin"
	"github.com/viant/bindly/provider/body"
	"github.com/viant/bindly/state"
)

// Bindly can read an authored input field and decode an entire named JSON
// object. These are sufficient for the common remote authorization shape.
func TestNativeRemoteBindingSupportsAuthShape(t *testing.T) {
	type input struct{ Token string }
	type request struct{ Authorization string }
	inputInjector, err := bindly.NewInjector(bindly.WithProviders(buildin.Struct("input", "", 0)))
	if err != nil {
		t.Fatal(err)
	}
	inputPlan, err := inputInjector.CompilePlan(reflect.TypeFor[request](), bindly.BindingSpec{
		Path: "Authorization", Location: state.Location{Kind: "input", In: "Token"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var outbound request
	if err := inputInjector.Bind(context.Background(), &outbound, bindly.WithPlan(inputPlan), bindly.WithSource(&input{Token: "Bearer alice"})); err != nil {
		t.Fatal(err)
	}
	if outbound.Authorization != "Bearer alice" {
		t.Fatalf("outbound Authorization = %q", outbound.Authorization)
	}

	type authContext struct {
		ID      int              `json:"id"`
		Allowed map[string][]int `json:"allowed"`
	}
	type output struct{ Context *authContext }
	source, err := body.New([]byte(`{"user":{"id":7,"allowed":{"project":[101,102]}}}`), "application/json", nil, body.WithExactFieldNames())
	if err != nil {
		t.Fatal(err)
	}
	outputInjector, err := bindly.NewInjector(bindly.WithProviders(source))
	if err != nil {
		t.Fatal(err)
	}
	required := true
	outputPlan, err := outputInjector.CompilePlan(reflect.TypeFor[output](), bindly.BindingSpec{
		Path: "Context", Location: state.Location{Kind: "body", In: "user"}, Required: &required,
	})
	if err != nil {
		t.Fatal(err)
	}
	var result output
	if err := outputInjector.Bind(context.Background(), &result, bindly.WithPlan(outputPlan)); err != nil {
		t.Fatal(err)
	}
	if result.Context == nil || result.Context.ID != 7 || !reflect.DeepEqual(result.Context.Allowed["project"], []int{101, 102}) {
		t.Fatalf("bound context = %#v", result.Context)
	}
}

// Current body Source treats Location.In as a literal top-level JSON key. A
// nested pointer must not be silently accepted as a required mapping.
func TestNativeRemoteBindingNestedDocumentPathGap(t *testing.T) {
	source, err := body.New([]byte(`{"outer":{"items":[{"/name":"Ada"}]}}`), "application/json", nil, body.WithExactFieldNames())
	if err != nil {
		t.Fatal(err)
	}
	injector, err := bindly.NewInjector(bindly.WithProviders(source))
	if err != nil {
		t.Fatal(err)
	}
	type output struct{ Name string }
	required := true
	for _, path := range []string{"/outer/items/0/~1name", "outer.items.0./name"} {
		_, found, err := source.Value(context.Background(), reflect.TypeFor[string](), path)
		if err != nil || found {
			t.Fatalf("body source path %q found=%v err=%v", path, found, err)
		}
		plan, err := injector.CompilePlan(reflect.TypeFor[output](), bindly.BindingSpec{
			Path: "Name", Location: state.Location{Kind: "body", In: path}, Required: &required,
		})
		if err != nil {
			t.Fatal(err)
		}
		var result output
		err = injector.Bind(context.Background(), &result, bindly.WithPlan(plan))
		if err == nil || result.Name != "" {
			t.Fatalf("nested path %q unexpectedly bound: result=%#v err=%v", path, result, err)
		}
	}
}

// CompilePlan's target is a Go struct. It has no target location for an
// arbitrary JSON body pointer such as /profile/name from an input field.
func TestNativeRemoteBindingCannotPlanDynamicBodyObject(t *testing.T) {
	injector, err := bindly.NewInjector()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := injector.CompilePlan(reflect.TypeFor[map[string]any](), bindly.BindingSpec{
		Path: "profile.name", Location: state.Location{Kind: "input", In: "Payload"},
	}); err == nil || !strings.Contains(err.Error(), "target must be a struct") {
		t.Fatalf("dynamic body target unexpectedly compiled: %v", err)
	}
}
