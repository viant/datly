package compiler

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestBuildBindingSpecsAcceptsParameterTagAlias(t *testing.T) {
	type input struct {
		VendorID int `parameter:"vendorID,kind=path,in=vendorID,required"`
	}

	bindings, err := BuildBindingSpecs(&spec.Component{}, reflect.TypeOf(input{}), nil)
	if err != nil {
		t.Fatalf("BuildBindingSpecs() error = %v", err)
	}
	if len(bindings) != 1 {
		t.Fatalf("expected one binding, got %+v", bindings)
	}
	actual := bindings[0]
	if actual.Path != "VendorID" || actual.Name != "vendorID" || actual.Location.Kind != "path" || actual.Location.In != "vendorID" || actual.Required == nil || !*actual.Required {
		t.Fatalf("unexpected parameter alias binding: %+v", actual)
	}
}

func TestBuildBindingSpecsPreservesCanonicalParamMetadata(t *testing.T) {
	type input struct {
		Fields []string
	}
	required := false
	cacheable := false
	value := "id,name"
	component := &spec.Component{Parameters: []*spec.Parameter{{
		Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "fields"},
		TypeExpr: "[]string", Cardinality: "Many", Required: &required, Cacheable: &cacheable,
		When: "enabled", Scope: "request", With: "Filter", ResourceRef: "assets:fields.sql", Value: &value, Async: true,
		ErrorStatusCode: 422, ErrorMessage: "bad fields",
	}}}
	bindings, err := BuildBindingSpecs(component, reflect.TypeOf(input{}), nil)
	if err != nil {
		t.Fatalf("BuildBindingSpecs() error = %v", err)
	}
	if len(bindings) != 1 {
		t.Fatalf("bindings = %+v", bindings)
	}
	actual := bindings[0]
	if actual.DataType != "[]string" || actual.Cardinality != "Many" || actual.Required == nil || *actual.Required || actual.Cacheable == nil || *actual.Cacheable || !actual.Async {
		t.Fatalf("unexpected type/flag metadata: %+v", actual)
	}
	if actual.When != "enabled" || actual.Scope != "request" || actual.With != "Filter" || actual.ResourceRef != "assets:fields.sql" || actual.DefaultValue != "id,name" {
		t.Fatalf("unexpected binding metadata: %+v", actual)
	}
	if actual.ErrorCode != 422 || actual.ErrorMessage != "bad fields" {
		t.Fatalf("unexpected error metadata: %+v", actual)
	}
}
