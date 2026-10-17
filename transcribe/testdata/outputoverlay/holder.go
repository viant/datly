package outputoverlay

import "github.com/viant/xdatly"

type Component struct {
	Contract xdatly.Component[Input, Output] `component:"Records,path=/overlay-records,method=GET,view=Records"`
}

func (Component) DatlyQuerySelectors() selectors { panic("metadata only") }

var Linked = new(Component)
