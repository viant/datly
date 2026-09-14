package reader

import (
	"github.com/viant/bindly/locator"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

func rootSelectors(selector xstate.Selector) []locator.Provider {
	return selectorProviders(xstate.Selectors{&xstate.NamedSelector{Selector: selector}})
}

func selectorProviders(selectors xstate.Selectors) []locator.Provider {
	return []locator.Provider{handlerprovider.Static(xhandler.SelectorsKey, selectors)}
}
