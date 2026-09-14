package tag

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestBindingAliasesDoesNotClaimParamDependency(t *testing.T) {
	type input struct {
		Events []int
		IDs    []int
	}
	field, _ := reflect.TypeOf(input{}).FieldByName("IDs")
	actual := BindingAliases(field, &spec.Parameter{
		Name: "IDs", Source: spec.BindSource{Kind: "param", Name: "Events"},
	})
	if len(actual) != 1 || actual[0] != "IDs" {
		t.Fatalf("aliases = %v", actual)
	}
}

func TestBindingAliasesIncludesTransportSource(t *testing.T) {
	type input struct{ TenantID int }
	field, _ := reflect.TypeOf(input{}).FieldByName("TenantID")
	actual := BindingAliases(field, &spec.Parameter{
		Name: "Tenant", Source: spec.BindSource{Kind: "query", Name: "tenant_id"},
	})
	if len(actual) != 3 || actual[0] != "TenantID" || actual[1] != "Tenant" || actual[2] != "tenant_id" {
		t.Fatalf("aliases = %v", actual)
	}
}
