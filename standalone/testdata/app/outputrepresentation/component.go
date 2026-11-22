package outputrepresentation

import (
	"context"

	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/xdatly"
	xhandler "github.com/viant/xdatly/handler"
)

type Input struct{}

type Output struct {
	OriginalValue int `json:"originalValue"`
}

type LinkedComponents struct {
	First  xdatly.Component[Input, Output] `component:"Represented,path=/represented,method=GET,handler=NewOutput" mcp:"[{\"kind\":\"tool\",\"name\":\"LinkedOutput_default\",\"caseFormat\":\"\"},{\"kind\":\"tool\",\"name\":\"LinkedOutput_u\",\"caseFormat\":\"u\"},{\"kind\":\"tool\",\"name\":\"LinkedOutput_l\",\"caseFormat\":\"l\"},{\"kind\":\"tool\",\"name\":\"LinkedOutput_d\",\"caseFormat\":\"d\"},{\"kind\":\"tool\",\"name\":\"LinkedOutput_ld\",\"caseFormat\":\"ld\"},{\"kind\":\"tool\",\"name\":\"LinkedOutput_ud\",\"caseFormat\":\"ud\"},{\"kind\":\"tool\",\"name\":\"LinkedOutput_lc\",\"caseFormat\":\"lc\"},{\"kind\":\"tool\",\"name\":\"LinkedOutput_uc\",\"caseFormat\":\"uc\"},{\"kind\":\"tool\",\"name\":\"LinkedOutput_lu\",\"caseFormat\":\"lu\"},{\"kind\":\"tool\",\"name\":\"LinkedOutput_uu\",\"caseFormat\":\"uu\"},{\"kind\":\"tool\",\"name\":\"LinkedOutput_t\",\"caseFormat\":\"t\"},{\"kind\":\"tool\",\"name\":\"LinkedOutput_s\",\"caseFormat\":\"s\"}]"`
	Second xdatly.Component[Input, Output] `component:"Represented,path=/represented-sibling,method=GET,handler=NewOutput"`
	Third  xdatly.Component[Input, Output] `component:"Canonical,path=/canonical,method=GET,handler=NewOutput"`
	Fourth xdatly.Component[Input, Output] `component:"Canonical,path=/canonical-sibling,method=GET,handler=NewOutput"`
}

func (LinkedComponents) DatlyHandler(name string) func() (handler.TypedHandler, error) {
	if name != "NewOutput" {
		return nil
	}
	return custom.Factory(NewOutput)
}

type outputHandler struct{}

func NewOutput() xhandler.Contract[Input, Output] { return &outputHandler{} }

func (*outputHandler) Exec(_ context.Context, _ xhandler.Session, _ *Input, output *Output) error {
	output.OriginalValue = 37
	return nil
}
