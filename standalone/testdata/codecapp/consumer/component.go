package consumer

import (
	"context"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/xdatly"
	"github.com/viant/xdatly/handler"
)

type Components struct {
	Probe xdatly.Component[Input, Output]       `component:"CodecProbe,path=/codec-probe,method=GET,handler=NewProbe" mcp:"[{\"kind\":\"tool\",\"name\":\"CodecProbe\"}]"`
	Read  xdatly.Component[Empty, ReaderOutput] `component:"CodecRows,path=/codec-rows,method=GET,connector=main" mcp:"[{\"kind\":\"tool\",\"name\":\"CodecRows\"}]"`
}

func (Components) DatlyHandler(name string) func() (rhandler.TypedHandler, error) {
	if name == "NewProbe" {
		return custom.Factory(NewProbe)
	}
	return nil
}

type Input struct {
	Values []int `parameter:"Values,kind=query,in=value,dataType=[]string" codec:"example.com/codecapp/transforms.QueryList,strict"`
}
type Output struct {
	Values []int `json:"values"`
}
type probe struct{}

func NewProbe() handler.Contract[Input, Output] { return &probe{} }
func (*probe) Exec(_ context.Context, _ handler.Session, input *Input, output *Output) error {
	output.Values = input.Values
	return nil
}

type Empty struct {
	Suffix string `parameter:"Suffix,kind=query,in=suffix"`
}
type ReaderOutput struct {
	Data []*Row   `parameter:",kind=output,in=view" view:"rows" sql:"SELECT id FROM records" json:"data"`
	Meta *Summary `parameter:",kind=output,in=summary" view:"summary" sql:"SELECT 'summary' AS label" json:"meta"`
}
type Row struct {
	ID    int    `sqlx:"id" json:"id"`
	Child *Child `view:"children" on:"ID:id=ID:id" sql:"SELECT id, label FROM records" json:"child"`
}
type Child struct {
	ID    int    `sqlx:"id" json:"id"`
	Label string `sqlx:"label,type=string" codec:"example.com/codecapp/transforms.WithSuffix" json:"label"`
}
type Summary struct {
	Label string `sqlx:"label,type=string" codec:"example.com/codecapp/transforms.Upper" json:"label"`
}
