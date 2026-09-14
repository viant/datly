package collector

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
)

func TestNestedRelationHolderFailsBeforeEvidenceCanAlias(t *testing.T) {
	type item struct{ ID int }
	type side struct{ Items []item }
	type parent struct{ Left, Right side }
	for _, holder := range []string{"Left.Items", "Right.Items"} {
		child := &data.View{Spec: spec.View{Name: "items"}}
		root := &data.View{Spec: spec.View{Name: "parent"}, Relations: []*data.Relation{{Name: holder, Holder: holder, Cardinality: spec.CardinalityMany, Of: &data.RelationRef{View: child}}}}
		_, err := Compile(root, map[*data.View]reflect.Type{root: reflect.TypeOf(parent{}), child: reflect.TypeOf(item{})})
		if err == nil || !strings.Contains(err.Error(), holder) {
			t.Fatalf("holder %s was not rejected explicitly: %v", holder, err)
		}
	}
}
