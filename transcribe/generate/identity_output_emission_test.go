package generate

import (
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	"reflect"
	"testing"
)

func TestCanonicalOutputIdentityMatchesExistingFieldEmission(t *testing.T) {
	for _, c := range []struct {
		kind, slot string
		emit       bool
	}{
		{"body", "", true}, {"body", "data", true}, {"output", "view", true}, {"output", "view", false}, {"output", "status", true}, {"output", "summary", false}, {"output", "derived", true}, {"logger", "", true}, {"const", "Flag", true}, {"component_invoker", "", true},
	} {
		p := &spec.Parameter{Name: "Data", TypeExpr: "string", Source: spec.BindSource{Kind: c.kind, Name: c.slot}, EmitOutput: c.emit, WireTypeExpr: "example.com/models.Wire"}
		emitted := fieldTag(p, p.Name, nil)
		meta, err := dtag.ParseField(reflect.StructField{Name: p.Name, Type: reflect.TypeFor[string](), Tag: reflect.StructTag(emitted)})
		if err != nil {
			t.Fatal(err)
		}
		reloaded := &spec.Parameter{Name: meta.Binding.Name, Source: spec.BindSource{Kind: meta.Binding.Location.Kind, Name: meta.Binding.Location.In}}
		if c.kind == "logger" || c.kind == "const" || c.kind == "component_invoker" {
			reloaded.EmitOutput = true
		}
		if p.Identity() != reloaded.Identity() {
			t.Fatalf("%+v -> %s: %s != %s", c, emitted, p.Identity(), reloaded.Identity())
		}
		if meta.WireType != p.WireTypeExpr || p.Source.Kind != c.kind || p.Source.Name != c.slot || p.EmitOutput != c.emit {
			t.Fatal("stored metadata changed")
		}
	}
}

func TestCanonicalIdentityRetainsCustomOutputAuthority(t *testing.T) {
	component, err := parseTestComponentSource("example.com/identity/records", "records", `#setting($_ = $route('/records','POST'))
#define($_ = $BodyEcho<string>(body/).Output())
SELECT 1`)
	if err != nil {
		t.Fatal(err)
	}
	before := component.Parameters[0].Identity()
	plan := testPlan(t, component)
	if len(plan.Input.Fields) != 0 || len(plan.Output.Fields) != 1 || plan.Output.Fields[0].Name != "BodyEcho" {
		t.Fatalf("implicit view/mutation owner appeared: %#v %#v", plan.Input.Fields, plan.Output.Fields)
	}
	p := component.Parameters[0]
	if p.Identity() != before || p.Source.Kind != "body" || p.Source.Name != "" || !p.EmitOutput || p.IsMutationInput() {
		t.Fatal("authored custom owner changed")
	}
}
