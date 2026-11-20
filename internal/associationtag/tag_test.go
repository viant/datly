package associationtag

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	type link struct {
		Id       int
		MemberId *string
		Fraction float64
	}
	type parent struct {
		Links []*link
	}
	holder, _ := reflect.TypeFor[parent]().FieldByName("Links")
	result, err := Parse(holder)
	if err != nil || result != nil {
		t.Fatalf("unannotated relation: %v %v", result, err)
	}
	holder.Tag = `association:"collection=creativeIds,member=MemberId"`
	result, err = Parse(holder)
	if err != nil || result.Collection != "creativeIds" || result.Member.Name != "MemberId" {
		t.Fatalf("explicit association: %v %v", result, err)
	}
	for _, tag := range []reflect.StructTag{
		`association:""`, `association:"member=Id"`, `association:"collection=ids"`,
		`association:"collection=ids,member=Missing"`, `association:"collection=ids,member=Fraction"`,
		`association:"collection=ids,member=Id,member=MemberId"`, `association:"member=Id,collection=ids,other=x"`,
	} {
		holder.Tag = tag
		if _, err := Parse(holder); err == nil {
			t.Errorf("accepted %s", tag)
		}
	}
}
