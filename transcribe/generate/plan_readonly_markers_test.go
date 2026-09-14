package generate

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestGeneratedPresenceExcludesReadOnlyRelations(t *testing.T) {
	for _, test := range []struct {
		name      string
		kind      spec.RelationKind
		auxiliary bool
	}{{"derived", spec.RelationKindDerived, false}, {"auxiliary", spec.RelationKindSubview, true}} {
		t.Run(test.name, func(t *testing.T) {
			component, root, child := setMarkerComponent()
			root.Relations[0].Kind = test.kind
			child.Auxiliary = test.auxiliary
			identity, _ := root.Identity()
			generated, err := New(Input{Component: component, SetMarkerViews: map[string]bool{identity: true}}).Plan()
			if err != nil {
				t.Fatal(err)
			}
			view := generatedViewByIdentity(generated, identity)
			if view == nil || !reflect.DeepEqual(view.SetMarkerFields, []string{"Id", "Name"}) {
				t.Fatalf("read-only relation got presence metadata: %+v", view)
			}
			if field, _ := view.Field("Items"); field.Name == "" {
				t.Fatal("read-only relation field was removed")
			}
		})
	}
}
