package provider

import (
	"reflect"

	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/locator/buildin"
	"github.com/viant/structology"
	xhandler "github.com/viant/xdatly/handler"
)

// CallerOutput anchors the authored output source to the calling component's value.
// Field matching remains owned by Bindly's native struct locator. The child's
// ordinary param/input providers continue to use its canonical input.
func CallerOutput(value any) locator.Provider {
	native := buildin.Struct(string(xhandler.CallerOutputKey), "", locator.PriorityTransform)
	var state *structology.State
	if value != nil {
		state = structology.NewStateType(reflect.TypeOf(value)).WithValue(value)
	}
	return &callerOutputProvider{Provider: native, source: native.Locate(state)}
}

type callerOutputProvider struct {
	locator.Provider
	source locator.Locator
}

func (p *callerOutputProvider) Locate(*structology.State) locator.Locator { return p.source }
