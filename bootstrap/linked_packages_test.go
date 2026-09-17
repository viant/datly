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

func TestDefaultImportsSnapshotsUserLinkedHolders(t *testing.T) {
	defaultImports.Lock()
	original := append([]any(nil), defaultImports.holders...)
	defaultImports.holders = nil
	defaultImports.Unlock()
	defer func() {
		defaultImports.Lock()
		defaultImports.holders = original
		defaultImports.Unlock()
	}()
	holders := []any{linkedWriterHolder{}, linkedReaderHolder{}}
	UseDefaultImports(holders...)
	actual := DefaultImports()
	if !reflect.DeepEqual(actual, holders) {
		t.Fatalf("default imports: got %#v want %#v", actual, holders)
	}
	actual[0] = linkedReaderHolder{}
	if reflect.DeepEqual(DefaultImports(), actual) {
		t.Fatal("caller mutated default import authority")
	}
}
