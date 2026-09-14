package sequencer

import (
	"reflect"
	"strings"
	"testing"
)

func TestWalkerCountEmpty(t *testing.T) {
	type Foo struct {
		ID   int
		Name string
	}
	type Bar struct {
		ID   int
		Foos []Foo
	}
	value := []*Bar{
		{ID: 1, Foos: []Foo{{ID: 1, Name: "abc1"}, {ID: 0, Name: "xyz1"}}},
		{ID: 2, Foos: []Foo{{ID: 2, Name: "abc2"}, {ID: 0, Name: "xyz2"}, {ID: 0, Name: "xxx"}}},
	}
	walker, err := NewWalker(value, []string{"Foos", "ID"})
	if err != nil {
		t.Fatalf("new walker failed: %v", err)
	}
	actual, err := walker.CountEmpty(value)
	if err != nil {
		t.Fatalf("count empty failed: %v", err)
	}
	if actual != 3 {
		t.Fatalf("expected 3 empty leaves, got %d", actual)
	}
}

func TestWalkerLeaf(t *testing.T) {
	type Foo struct {
		ID   int
		Name string
	}
	type Bar struct {
		ID   int
		Foos []Foo
	}
	value := []*Bar{
		{ID: 1, Foos: []Foo{{ID: 1, Name: "abc1"}, {ID: 0, Name: "xyz1"}}},
	}
	walker, err := NewWalker(value, []string{"Foos", "ID"})
	if err != nil {
		t.Fatalf("new walker failed: %v", err)
	}
	actual, err := walker.Leaf(value)
	if err != nil {
		t.Fatalf("leaf failed: %v", err)
	}
	typed, ok := actual.(*Foo)
	if !ok {
		t.Fatalf("unexpected leaf type %T", actual)
	}
	if typed.ID != 1 || typed.Name != "abc1" {
		t.Fatalf("unexpected leaf %+v", typed)
	}
}

func TestWalkerEmptyLeaf(t *testing.T) {
	type Foo struct {
		ID   int
		Name string
	}
	type Bar struct {
		ID   int
		Foos []Foo
	}
	value := []*Bar{
		{ID: 1, Foos: []Foo{{ID: 10, Name: "keep"}, {ID: 0, Name: "allocate-me"}}},
	}
	walker, err := NewWalker(value, []string{"Foos", "ID"})
	if err != nil {
		t.Fatalf("new walker failed: %v", err)
	}
	actual, err := walker.EmptyLeaf(value)
	if err != nil {
		t.Fatalf("empty leaf failed: %v", err)
	}
	typed, ok := actual.(*Foo)
	if !ok {
		t.Fatalf("unexpected empty leaf type %T", actual)
	}
	if typed.Name != "allocate-me" {
		t.Fatalf("unexpected empty leaf %+v", typed)
	}
}

func TestWalkerAllocatesEveryCanonicalIntegerKind(t *testing.T) {
	integerTypes := []reflect.Type{
		reflect.TypeFor[int](), reflect.TypeFor[int8](), reflect.TypeFor[int16](), reflect.TypeFor[int32](), reflect.TypeFor[int64](),
		reflect.TypeFor[uint](), reflect.TypeFor[uint8](), reflect.TypeFor[uint16](), reflect.TypeFor[uint32](), reflect.TypeFor[uint64](),
	}
	for _, integerType := range integerTypes {
		for _, fieldType := range []reflect.Type{integerType, reflect.PointerTo(integerType)} {
			name := fieldType.String()
			t.Run(name, func(t *testing.T) {
				ownerType := reflect.StructOf([]reflect.StructField{{Name: "ID", Type: fieldType}})
				owner := reflect.New(ownerType)
				walker, err := NewWalker(owner.Interface(), []string{"ID"})
				if err != nil {
					t.Fatalf("NewWalker() error = %v", err)
				}
				sequence := &Sequence{Value: 7, IncrementBy: 1}
				if err = walker.Allocate(owner.Interface(), sequence); err != nil {
					t.Fatalf("Allocate() error = %v", err)
				}
				actual := owner.Elem().Field(0)
				if actual.Kind() == reflect.Pointer {
					if actual.IsNil() {
						t.Fatal("Allocate() left pointer key nil")
					}
					actual = actual.Elem()
				}
				if actual.Kind() >= reflect.Int && actual.Kind() <= reflect.Int64 {
					if actual.Int() != 7 {
						t.Fatalf("allocated value = %d, want 7", actual.Int())
					}
				} else if actual.Uint() != 7 {
					t.Fatalf("allocated value = %d, want 7", actual.Uint())
				}
			})
		}
	}
}

func TestWalkerRejectsSequenceOverflow(t *testing.T) {
	value := &struct{ ID *int8 }{}
	walker, err := NewWalker(value, []string{"ID"})
	if err != nil {
		t.Fatalf("NewWalker() error = %v", err)
	}
	err = walker.Allocate(value, &Sequence{Value: 128, IncrementBy: 1})
	if err == nil || !strings.Contains(err.Error(), "overflows int8") {
		t.Fatalf("Allocate() error = %v", err)
	}
	if value.ID != nil {
		t.Fatalf("failed allocation materialized key pointer: %v", *value.ID)
	}
}

func TestWalkerPreservesLargeUnsignedKeysWhileAllocatingEmptyKeys(t *testing.T) {
	type record struct {
		ID uint64
	}
	values := []*record{{ID: ^uint64(0)}, {}}
	walker, err := NewWalker(values, []string{"ID"})
	if err != nil {
		t.Fatalf("NewWalker() error = %v", err)
	}
	count, err := walker.CountEmpty(values)
	if err != nil {
		t.Fatalf("CountEmpty() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("CountEmpty() = %d, want 1", count)
	}
	if err = walker.Allocate(values, &Sequence{Value: 7, IncrementBy: 1}); err != nil {
		t.Fatalf("Allocate() error = %v", err)
	}
	if values[0].ID != ^uint64(0) || values[1].ID != 7 {
		t.Fatalf("allocated IDs = %v, want [%d 7]", []uint64{values[0].ID, values[1].ID}, ^uint64(0))
	}
}

func TestNewWalkerRejectsUntypedNil(t *testing.T) {
	if _, err := NewWalker(nil, []string{"ID"}); err == nil {
		t.Fatal("NewWalker(nil) error = nil, want an error")
	}
}
