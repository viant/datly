package companionpagecallbacks

import (
	"context"
	"github.com/viant/xdatly"
	xhandler "github.com/viant/xdatly/handler"
	xmcp "github.com/viant/xdatly/handler/mcp"
)

type InvokerInput struct {
	ID int `parameter:"ID,kind=query,in=id,required=false"`
}

func (*InvokerInput) Init(context.Context, xhandler.ComponentInvoker) error {
	panic("unsupported callback invoked")
}

type MCPInput struct {
	ID int `parameter:"ID,kind=query,in=id,required=false"`
}

func (*MCPInput) InitMCP(context.Context, xmcp.Context) error { panic("unsupported callback invoked") }

type Output struct {
	Data []*Row `parameter:"Data,kind=output,in=view" view:"rows,type=Row" sql:"SELECT r.* FROM (SELECT id FROM records) r"`
}
type Row struct {
	ID int `sqlx:"id"`
}
type selectors struct {
	Fields []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=rows,property=fields"`
	Page   int      `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=rows,property=page"`
}
type InvokerComponent struct {
	Contract xdatly.Component[InvokerInput, Output] `component:"InvokerPage,path=/invoker-page,method=GET" ignoreEmptyQueryParameters:"false"`
}

func (InvokerComponent) DatlyQuerySelectors() selectors { panic("metadata method invoked") }

type MCPComponent struct {
	Contract xdatly.Component[MCPInput, Output] `component:"MCPPage,path=/mcp-page,method=GET" ignoreEmptyQueryParameters:"false"`
}

func (MCPComponent) DatlyQuerySelectors() selectors { panic("metadata method invoked") }

var LinkedInvoker = new(InvokerComponent)
var LinkedMCP = new(MCPComponent)
