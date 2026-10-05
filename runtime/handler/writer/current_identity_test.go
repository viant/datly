package writer

import (
	"reflect"
	"strings"
	"testing"
)

func TestCurrentScalarIdentityToSparsePointer(t *testing.T) {
	type entity struct {
		ID   *int
		Name *string
	}
	type current struct {
		ID   int
		Name string
	}
	record := &Record{Path: "Delete", EntityType: reflect.TypeFor[entity](), Fields: []Field{{Name: "ID", Index: []int{0}}, {Name: "Name", Index: []int{1}}}, Keys: []Field{{Name: "ID", Index: []int{0}}}}
	for _, id := range []int{0, 7} {
		program := &Program{database: &DatabaseSnapshot{Rows: map[rowIdentity]reflect.Value{}, ByRecord: map[*Record][]reflect.Value{}}}
		row := &current{ID: id, Name: "original"}
		if err := program.indexCurrent(record, reflect.ValueOf([]*current{nil, row})); err != nil {
			t.Fatal(err)
		}
		previous := program.database.ByRecord[record][0].Interface().(*entity)
		if previous.ID == nil || *previous.ID != id || previous.Name == nil || *previous.Name != "original" {
			t.Fatalf("lost loaded values: %+v", previous)
		}
		row.ID = 99
		row.Name = "changed"
		if *previous.ID != id || *previous.Name != "original" {
			t.Fatal("previous aliased mutable current row")
		}
		if err := program.indexCurrent(record, reflect.ValueOf([]*current{{ID: id}})); err == nil || !strings.Contains(err.Error(), "duplicated") {
			t.Fatalf("duplicate identity: %v", err)
		}
	}
	type incompatible struct{ ID string }
	program := &Program{database: &DatabaseSnapshot{Rows: map[rowIdentity]reflect.Value{}, ByRecord: map[*Record][]reflect.Value{}}}
	if err := program.indexCurrent(record, reflect.ValueOf([]*incompatible{{ID: "7"}})); err == nil || !strings.Contains(err.Error(), "incomplete identity") {
		t.Fatalf("incompatible type accepted: %v", err)
	}
}
