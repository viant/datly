package compiler

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

type independentBodyLeaf struct{ Id int }
type independentBodyOther struct{ Id int }
type independentBodyParent struct {
	Children []*independentBodyLeaf `view:"children,type=independentBodyLeaf,table=children" on:"Id=ParentId"`
	Other    *independentBodyOther
}
type independentBodyInput struct {
	Data []*independentBodyParent `parameter:"Data,kind=body,in=data" view:"parents,type=independentBodyParent,table=parents"`
}

func TestIndependentBodyTypeAuthority(t *testing.T) {
	for _, tc := range []struct {
		name, operation, handler, root string
		target                         reflect.Type
		want                           bool
	}{
		{"shared parent", "patch", "", "parents", reflect.TypeFor[[]*independentBodyParent](), true},
		{"declared child", "patch", "", "parents", reflect.TypeFor[[]*independentBodyLeaf](), true},
		{"unrelated field", "patch", "", "parents", reflect.TypeFor[[]*independentBodyOther](), false},
		{"ordinary reader", "", "", "parents", reflect.TypeFor[[]*independentBodyParent](), false},
		{"custom handler", "patch", "custom", "parents", reflect.TypeFor[[]*independentBodyParent](), false},
		{"different root", "patch", "", "other", reflect.TypeFor[[]*independentBodyParent](), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &spec.Component{Settings: &spec.Settings{Mutation: tc.operation}, RootView: &spec.View{Name: tc.root}, Routes: []*spec.Route{{Handler: tc.handler}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "body", Name: "data"}}}}
			rows, err := independentBodyTypes(component, reflect.TypeFor[independentBodyInput](), tc.target)
			if err != nil {
				t.Fatal(err)
			}
			if got := len(rows) > 0; got != tc.want {
				t.Fatalf("shared body authority=%v want %v", got, tc.want)
			}
		})
	}
}

func TestIndependentInternalBodyAuthority(t *testing.T) {
	for _, kind := range []string{"internal", "transient"} {
		component := &spec.Component{Settings: &spec.Settings{Mutation: "patch"}, RootView: &spec.View{Name: "parents"}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: kind}}}}
		rows, err := independentBodyTypes(component, reflect.TypeFor[independentBodyInput](), reflect.TypeFor[[]*independentBodyParent]())
		if err != nil {
			t.Fatal(err)
		}
		if got := len(rows) > 0; got != (kind == "internal") {
			t.Fatalf("%s authority=%v", kind, got)
		}
	}
}
