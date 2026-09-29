package compiler

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestWriterSelectsBodyAlongsideMetadataOutput(t *testing.T) {
	component := &spec.Component{Parameters: []*spec.Parameter{
		{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "body"}},
		{Name: "Created", Source: spec.BindSource{Kind: "output", Name: "created"}},
	}}
	selected, err := (&compiler{}).selectOutput(component, "")
	if err != nil || selected.Name != "Data" {
		t.Fatalf("selection=%+v error=%v", selected, err)
	}
	selected, err = (&compiler{}).selectOutput(component, "Created")
	if err != nil || selected.Name != "Created" {
		t.Fatalf("explicit selection=%+v error=%v", selected, err)
	}
	component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Other", Source: spec.BindSource{Kind: "output", Name: "body"}})
	if _, err = (&compiler{}).selectOutput(component, ""); err == nil {
		t.Fatal("multiple body outputs were accepted")
	}
}
