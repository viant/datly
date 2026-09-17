package standalone

import (
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap/index"
)

func TestIndexedMaterializerTypeSelectionIncludesOnlyDependencyGoPackages(t *testing.T) {
	ownerScope := "example.com/app/tool"
	sources := []index.Source{
		{Kind: index.SourceGo, PackagePath: "example.com/app/auth"},
		{Kind: index.SourceDQL, PackagePath: "example.com/app/tool"},
		{Kind: index.SourceGo, PackagePath: "github.com/viant/xdatly/response"},
		{Kind: index.SourceResource, PackagePath: "example.com/app/tool"},
		{Kind: index.SourceGo, PackagePath: "example.com/app/auth"},
		{Kind: index.SourceGo, PackagePath: "example.com/app/tool"},
	}

	actual := indexedMaterializerTypeSelection(ownerScope, sources)
	expected := []string{
		"example.com/app/auth",
		"github.com/viant/xdatly/response",
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("selection = %v, want %v", actual, expected)
	}
}
