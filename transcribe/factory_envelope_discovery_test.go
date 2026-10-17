package transcribe

import (
	"github.com/viant/datly/spec"
	"testing"
)

func TestFactoryRelationOnlyEnvelopeDiscovery(t *testing.T) {
	child := &spec.View{TypeName: "Record", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "ID", Type: spec.TypeRef{Name: "int"}}}}
	root := &spec.View{TypeName: "Body", Source: &spec.ViewSource{}, Relations: []*spec.Relation{{View: child}, {View: child}}}
	if err := validateFactoryDiscoveredShape(root); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"empty", "physical root", "external root", "empty descendant", "nil relation", "cycle", "untyped descendant"} {
		t.Run(name, func(t *testing.T) {
			leaf := &spec.View{TypeName: "Record", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "ID", Type: spec.TypeRef{Name: "int"}}}}
			candidate := &spec.View{TypeName: "Body", Source: &spec.ViewSource{}, Relations: []*spec.Relation{{View: leaf}}}
			switch name {
			case "empty":
				candidate.Relations = nil
			case "physical root":
				candidate.Source.Table = "records"
			case "external root":
				candidate.TypeName = "other.Body"
			case "empty descendant":
				leaf.Columns = nil
			case "nil relation":
				candidate.Relations = append(candidate.Relations, nil)
			case "cycle":
				leaf.Relations = []*spec.Relation{{View: candidate}}
			case "untyped descendant":
				leaf.Columns[0].Type = spec.TypeRef{}
			}
			if err := validateFactoryDiscoveredShape(candidate); err == nil {
				t.Fatal("invalid envelope accepted")
			}
		})
	}
}
