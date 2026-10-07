package runtime

import (
	"testing"

	"github.com/viant/bindly/locator"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

func TestComponentDependenciesDoNotInheritParentReaderSelectors(t *testing.T) {
	header := handlerprovider.Static("header", "verified credential")
	query := handlerprovider.Static("query", "ordinary query values")
	selector := handlerprovider.Static(xhandler.SelectorsKey, xstate.Selectors{&xstate.NamedSelector{Name: "records"}})
	parent := componentScope{providers: []locator.Provider{header, query, selector}}
	child := componentDependencyScope(parent)
	if got := child.Providers(); len(got) != 2 || got[0] != header || got[1] != query {
		t.Fatalf("nonselector invocation authority changed: %+v", got)
	}
	// The report can still install selectors explicitly for its exact source.
	explicit, err := handlerengine.ComposeScope(child, selector)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, provider := range explicit.Providers() {
		found = found || provider.Kind() == string(xhandler.SelectorsKey)
	}
	if !found {
		t.Fatal("targeted report selectors were removed")
	}
	for _, provider := range componentDependencyScope(explicit).Providers() {
		if provider.Kind() == string(xhandler.SelectorsKey) {
			t.Fatal("source records selector leaked to private profile dependency")
		}
	}
	if len(parent.Providers()) != 3 {
		t.Fatal("inherited parent scope was mutated")
	}
}
