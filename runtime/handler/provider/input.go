package provider

import (
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/locator/buildin"
	xhandler "github.com/viant/xdatly/handler"
)

// Input exposes the canonical component input and its named fields from the
// active Bindly source state.
func Input() locator.Provider {
	return buildin.Struct(string(xhandler.InputKey), "", locator.PriorityTransform)
}
