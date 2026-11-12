package contract

import (
	"testing"

	"github.com/viant/datly/view/state"
)

func TestUpdateOutputParameterTypeNormalizesAnonymousStatusName(t *testing.T) {
	parameter := &state.Parameter{
		Name: "ResponseStatus",
		In:   state.NewOutputLocation("status"),
		Tag:  `anonymous:"true"`,
	}
	UpdateOutputParameterType(parameter)
	if parameter.Name != "Status" {
		t.Fatalf("anonymous status name = %q, want Status", parameter.Name)
	}
	if parameter.Schema == nil || parameter.Schema.Name != "response.Status" {
		t.Fatalf("unexpected status schema: %#v", parameter.Schema)
	}
}

func TestUpdateOutputParameterTypePreservesNamedStatus(t *testing.T) {
	parameter := &state.Parameter{
		Name: "ResponseStatus",
		In:   state.NewOutputLocation("status"),
	}
	UpdateOutputParameterType(parameter)
	if parameter.Name != "ResponseStatus" {
		t.Fatalf("named status name = %q, want ResponseStatus", parameter.Name)
	}
}
