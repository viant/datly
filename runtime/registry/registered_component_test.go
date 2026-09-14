package registry

import (
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	docs "github.com/viant/datly/documentation"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
)

func TestRegisteredComponentRetainsOnlyApprovedRuntimeCarrierFields(t *testing.T) {
	typeOf := reflect.TypeOf(RegisteredComponent{})
	expected := map[string]reflect.Type{
		"Documentation": reflect.TypeFor[*docs.Snapshot](),
		"Component":     reflect.TypeFor[*spec.Component](),
		"Input":         reflect.TypeFor[*InputContract](),
		"OutputType":    reflect.TypeFor[reflect.Type](),
		"Output":        reflect.TypeFor[*OutputContract](),
		"Reader":        reflect.TypeFor[dexec.Reader](),
		"Handler":       reflect.TypeFor[rhandler.Handler](),
		"Capabilities":  reflect.TypeFor[rhandler.InvocationCapabilities](),
		"Providers":     reflect.TypeFor[[]locator.Provider](),
		"DataSource":    reflect.TypeFor[dexec.DataSource](),
	}
	if typeOf.NumField() != len(expected) {
		t.Fatalf("RegisteredComponent has %d fields, want %d", typeOf.NumField(), len(expected))
	}
	for name, expectedType := range expected {
		field, ok := typeOf.FieldByName(name)
		if !ok || field.Type != expectedType {
			t.Fatalf("field %s = (%v, %v), want %v", name, field.Type, ok, expectedType)
		}
	}
}
