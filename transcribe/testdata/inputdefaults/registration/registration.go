package registration

import (
	c "github.com/viant/datly/transcribe/testdata/inputdefaults/contracts"
	"github.com/viant/xdatly"
)

type ComponentA struct {
	Route xdatly.Component[c.InputA, c.Output] `component:"ReadA,path=/read-a,method=GET,connector=main,view=Rows"`
}
type ComponentB struct {
	Route xdatly.Component[c.InputB, c.Output] `component:"ReadB,path=/read-b,method=GET,connector=main,view=Rows"`
}
