package compiler

import (
	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestResolutionGroupCanonicalTagAgreementAndIsolation(t *testing.T) {
	type input struct {
		ID int `bind:"ID,kind=path,in=id,resolutionGroup=archive,resolutionAfter=Jwt|Data"`
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "ID", Source: spec.BindSource{Kind: "path", Name: "id"}, ResolutionGroup: &spec.ResolutionGroupSpec{Name: "archive", After: []string{"Jwt", "Data"}}}}}
	bindings, err := BuildBindingSpecs(component, reflect.TypeFor[input](), nil)
	if err != nil {
		t.Fatal(err)
	}
	component.Parameters[0].ResolutionGroup.After[0] = "changed"
	if bindings[0].ResolutionGroup.After[0] != "Jwt" {
		t.Fatal("compiled group shares authoring metadata")
	}
	if _, err = BuildBindingSpecs(component, reflect.TypeFor[input](), nil); err == nil {
		t.Fatal("canonical/tag disagreement accepted")
	}
	component.Parameters[0].ResolutionGroup = nil
	if _, err = BuildBindingSpecs(component, reflect.TypeFor[input](), nil); err == nil {
		t.Fatal("generated-only group accepted against canonical declaration")
	}
}

func TestReadOnlyGroupCanonicalTagAgreement(t *testing.T) {
	type input struct {
		Child int `parameter:"Child,kind=component,in=GET:/child,resolutionGroup=reads,resolutionMode=dependencies"`
	}
	component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Child", Source: spec.BindSource{Kind: "component", Name: "GET:/child"}, ResolutionGroup: &spec.ResolutionGroupSpec{Name: "reads", Mode: "dependencies"}}}}
	bindings, err := BuildBindingSpecs(component, reflect.TypeFor[input](), nil)
	if err != nil || bindings[0].ResolutionGroup.Mode != bindly.ResolutionGroupDependencies {
		t.Fatalf("read-only mode lost: %+v %v", bindings, err)
	}
	component.Parameters[0].ResolutionGroup.Mode = ""
	if _, err := BuildBindingSpecs(component, reflect.TypeFor[input](), nil); err == nil {
		t.Fatal("canonical mode disagreement accepted")
	}
}
func TestResolutionGroupUnsupportedMetadataRejected(t *testing.T) {
	for _, param := range []*spec.Parameter{{Async: true}, {When: "enabled"}, {Codec: &spec.Codec{Body: "adapt"}}, {EmitOutput: true}} {
		param.Name = "ID"
		param.ResolutionGroup = &spec.ResolutionGroupSpec{Name: "archive", After: []string{"Jwt", "Data"}}
		if _, err := compileResolutionGroup(param, bindly.BindingSpec{}, false); err == nil {
			t.Fatalf("unsupported group %+v accepted", param)
		}
	}
}
