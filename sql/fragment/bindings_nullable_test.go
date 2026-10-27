package fragment

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"
)

func TestBindingsNullableTypeMetadataIsImmutable(t *testing.T) {
	types := []reflect.Type{reflect.TypeFor[*time.Time]()}
	bindings := NewBindings(types...)
	types[0] = reflect.TypeFor[*int]()
	placeholder, err := bindings.AddNullable(nil, 0)
	if err != nil || placeholder != "?" {
		t.Fatalf("nullable placeholder=%q error=%v", placeholder, err)
	}
	args := bindings.Args()
	value, ok := args[0].(*time.Time)
	if !ok || value != nil {
		t.Fatalf("nullable argument=%T; want typed nil date", args[0])
	}
	for _, index := range []int{-1, 1} {
		if _, err := bindings.AddNullable(nil, index); err == nil {
			t.Fatal("invalid type index accepted")
		}
	}
	if _, err := NewBindings(reflect.TypeFor[time.Time]()).AddNullable(nil, 0); err == nil {
		t.Fatal("nonnullable type accepted")
	}
	if len(bindings.Args()) != 1 {
		t.Fatal("failed binding appended an argument")
	}
}

type nullableBindingValuer struct{}

func (*nullableBindingValuer) Value() (driver.Value, error) {
	panic("binding must not execute a driver valuer")
}

func TestBindingsNullablePreservesOrdinaryAndValuerArguments(t *testing.T) {
	bindings := NewBindings(reflect.TypeFor[*nullableBindingValuer]())
	var nullable *nullableBindingValuer
	if _, err := bindings.AddNullable(nullable, 0); err != nil {
		t.Fatal(err)
	}
	value := &nullableBindingValuer{}
	if _, err := bindings.AddNullable(value, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := bindings.Add("ordinary"); err != nil {
		t.Fatal(err)
	}
	args := bindings.Args()
	if actual, ok := args[0].(*nullableBindingValuer); !ok || actual != nil {
		t.Fatal("typed nil valuer changed")
	}
	if args[1] != value || args[2] != "ordinary" {
		t.Fatal("existing binding behavior changed")
	}
}
