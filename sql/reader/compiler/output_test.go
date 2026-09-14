package compiler

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestResolveOutputField(t *testing.T) {
	type output struct {
		Data    any
		Summary any
		Status  string
		Metrics any
		Custom  any
	}
	outputType := reflect.TypeOf(output{})

	t.Run("missing or pointer output type", func(t *testing.T) {
		if actual := ResolveOutputField(nil, nil, ViewSlot); actual != "" {
			t.Fatalf("expected no field for nil output, got %q", actual)
		}
		if actual := ResolveOutputField(nil, reflect.TypeOf((*output)(nil)), ViewSlot); actual != "Data" {
			t.Fatalf("expected pointer output to resolve Data, got %q", actual)
		}
	})

	t.Run("explicit output param wins", func(t *testing.T) {
		component := &spec.Component{
			Parameters: []*spec.Parameter{
				{Name: "Custom", Source: spec.BindSource{Kind: "output", Name: "view"}},
			},
		}
		if actual := ResolveOutputField(component, outputType, ViewSlot); actual != "Custom" {
			t.Fatalf("expected Custom, got %q", actual)
		}
	})

	t.Run("conventional field blocked when shadowed by non-output param", func(t *testing.T) {
		component := &spec.Component{
			Parameters: []*spec.Parameter{
				{Name: "Status", Source: spec.BindSource{Kind: "query", Name: "status"}},
			},
		}
		if actual := ResolveOutputField(component, outputType, StatusSlot); actual != "" {
			t.Fatalf("expected empty conventional resolution due to shadowing, got %q", actual)
		}
	})

	t.Run("conventional field blocked when emit output param reuses field", func(t *testing.T) {
		component := &spec.Component{
			Parameters: []*spec.Parameter{
				{Name: "Metrics", Source: spec.BindSource{Kind: "output", Name: "other"}, EmitOutput: true},
			},
		}
		if actual := ResolveOutputField(component, outputType, MetricsSlot); actual != "" {
			t.Fatalf("expected empty conventional resolution due to emitOutput shadowing, got %q", actual)
		}
	})
}
