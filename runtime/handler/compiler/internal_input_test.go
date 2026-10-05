package compiler

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

type internalDeleteInput struct {
	Id     int                     `parameter:"Id,kind=path,in=id"`
	Delete []*internalDeleteRecord `parameter:"Delete,kind=internal,in=" json:"-" internal:"true"`
}
type internalDeleteRecord struct{ Id int }

func TestInternalMutationRootHasNoBinding(t *testing.T) {
	component := &spec.Component{Parameters: []*spec.Parameter{
		{Name: "Id", Source: spec.BindSource{Kind: "path", Name: "id"}},
		{Name: "Delete", Source: spec.BindSource{Kind: "internal"}},
	}}
	for _, c := range []*spec.Component{component, {}} {
		bindings, err := BuildBindingSpecs(c, reflect.TypeOf(internalDeleteInput{}), nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(bindings) != 1 || bindings[0].Path != "Id" {
			t.Fatalf("internal collection has external binding: %+v", bindings)
		}
	}
}
