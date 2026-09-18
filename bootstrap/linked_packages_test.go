package bootstrap

import (
	"reflect"
	"testing"
)

type linkedReaderHolder struct{}
type linkedWriterHolder struct{}

func TestLinkedPackagePathsAreDerivedAndSorted(t *testing.T) {
	holders := []any{linkedWriterHolder{}, linkedReaderHolder{}, linkedWriterHolder{}}
	want := []string{"github.com/viant/datly/bootstrap"}
	if actual := LinkedPackagePaths(holders); !reflect.DeepEqual(actual, want) {
		t.Fatalf("linked packages=%v want=%v", actual, want)
	}
}

func TestLinkedHolderUsesRuntimePackageTypes(t *testing.T) {
	typeOf := reflect.TypeOf(linkedReaderHolder{})
	holder := LinkedHolder(nil, typeOf.PkgPath(), typeOf.Name())
	actual := reflect.TypeOf(holder)
	if actual == nil || actual.Kind() != reflect.Pointer || actual.Elem() != typeOf {
		t.Fatalf("linked holder type = %v, want *%v", actual, typeOf)
	}
}
