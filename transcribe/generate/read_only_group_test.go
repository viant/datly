package generate

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/datly/spec"
)

func TestReadOnlyGroupGeneratedTag(t *testing.T) {
	parameter := &spec.Parameter{Name: "Child", Source: spec.BindSource{Kind: "component", Name: "GET:/child"}, ResolutionGroup: &spec.ResolutionGroupSpec{Name: "reads", Mode: "dependencies", After: []string{"Auth"}, DependsOn: []string{"First"}}}
	tag := fieldTag(parameter, "Child", nil)
	if !strings.Contains(tag, "resolutionMode=dependencies") {
		t.Fatal(tag)
	}
	parsed, found, err := bindly.BindingSpecFromField(reflect.StructField{Name: "Child", Type: reflect.TypeFor[int](), Tag: reflect.StructTag(tag)})
	if err != nil || !found || parsed.ResolutionGroup == nil || parsed.ResolutionGroup.Mode != bindly.ResolutionGroupDependencies || parsed.ResolutionGroup.After[0] != "Auth" || parsed.ResolutionGroup.DependsOn[0] != "First" {
		t.Fatalf("generated group does not round trip: %s %+v %v", tag, parsed, err)
	}
	if fieldTag(parameter, "Child", nil) != tag {
		t.Fatal("unstable generated group tag")
	}
}
