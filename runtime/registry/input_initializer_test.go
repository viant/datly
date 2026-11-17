package registry

import (
	"context"
	"reflect"
	"testing"
)

type InitializerOne struct{}

func (*InitializerOne) Init(context.Context) error { return nil }

type InitializerTwo struct{}

func (*InitializerTwo) Init(context.Context) error { return nil }

type NamedInitializer struct{ InitializerOne }

func TestEmbeddedInitializerDiscovery(t *testing.T) {
	one := reflect.StructField{Name: "InitializerOne", Type: reflect.TypeFor[InitializerOne](), Anonymous: true}
	two := reflect.StructField{Name: "InitializerTwo", Type: reflect.TypeFor[InitializerTwo](), Anonymous: true}
	path, err := compileEmbeddedInitializer(reflect.StructOf([]reflect.StructField{one}))
	if err != nil || !reflect.DeepEqual(path, []int{0}) {
		t.Fatalf("path=%v err=%v", path, err)
	}
	_, err = compileEmbeddedInitializer(reflect.StructOf([]reflect.StructField{one, two}))
	if err == nil {
		t.Fatal("ambiguous initializer accepted")
	}
	path, err = compileEmbeddedInitializer(reflect.TypeFor[NamedInitializer]())
	if err != nil || path != nil {
		t.Fatal("named input lifecycle changed")
	}
	one.Anonymous = false
	path, err = compileEmbeddedInitializer(reflect.StructOf([]reflect.StructField{one}))
	if err != nil || path != nil {
		t.Fatal("ordinary field implicitly initialized")
	}
}
