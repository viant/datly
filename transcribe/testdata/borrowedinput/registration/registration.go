package registration

import (
	"github.com/viant/datly/transcribe/testdata/borrowedinput/contracts"
	"github.com/viant/xdatly"
)

type Component struct {
	Route xdatly.Component[contracts.Input, contracts.Output] `component:"Read,path=/borrowed,method=GET"`
}
