package records

import (
	"embed"
	hooks "example.com/buildapp/hooks"
	models "example.com/buildmodel"
	rhandler "github.com/viant/datly/runtime/handler"
	customhandler "github.com/viant/datly/runtime/handler/custom"
	xdatly "github.com/viant/xdatly"
	"reflect"
)

// The application declares only real component metadata, with no registration.
type Component struct {
	Read  xdatly.Component[Input, Output]             `component:"Read,path=/records/{id},method=GET,connector=main,view=records"`
	Write xdatly.Component[hooks.Input, hooks.Output] `component:"Write,path=/records,method=POST,connector=main,handler=hooks.NewWrite"`
}

var RecordsDatly = new(Component)
var ComponentType = reflect.TypeOf((*Component)(nil)).Elem()

type Input struct {
	ID int `parameter:"ID,kind=path,in=id,required"`
}
type Output struct {
	Rows []*models.Record `parameter:"Rows,kind=output,in=view" view:"records,table=records" sql:"uri=build_records:queries/read.sql" json:"rows"`
}

//go:embed queries/*.sql
var Assets embed.FS

func (Component) EmbedFS() *embed.FS { return &Assets }

func (Component) EmbedNamespace() string { return "build_records" }

func (Component) DatlyHandler(name string) func() (rhandler.TypedHandler, error) {
	if name == "hooks.NewWrite" || name == "example.com/buildapp/hooks.NewWrite" {
		return customhandler.Factory[hooks.Input, hooks.Output](hooks.NewWrite)
	}
	return nil
}

var RecordsHandler = Component{}.DatlyHandler
