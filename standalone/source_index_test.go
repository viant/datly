package standalone

import (
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap/index"
	"github.com/viant/datly/spec"
)

func TestIndexedMaterializerSelectionIncludesEntrySourcePackages(t *testing.T) {
	owner := spec.Key{Kind: spec.KindComponent, Scope: "example.com/app/tool", Name: "Tool"}
	sources := []index.Source{
		{Kind: index.SourceGo, PackagePath: "example.com/app/auth"},
		{Kind: index.SourceDQL, PackagePath: "example.com/app/tool"},
		{Kind: index.SourceGo, PackagePath: "github.com/viant/xdatly/response"},
		{Kind: index.SourceResource, PackagePath: "example.com/app/tool"},
		{Kind: index.SourceGo, PackagePath: "example.com/app/auth"},
	}

	actual := indexedMaterializerSelection(owner, sources)
	expected := []string{
		"example.com/app/auth",
		"example.com/app/tool",
		"github.com/viant/xdatly/response",
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("selection = %v, want %v", actual, expected)
	}
}
