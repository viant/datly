package records

import (
	"embed"
	hooks "example.com/buildapp/hooks"
	models "example.com/buildmodel"
	xdatly "github.com/viant/xdatly"
)

// The application declares only real component metadata, with no registration.
type Component struct {
	Read  xdatly.Component[Input, Output]             `component:"Read,path=/records/{id},method=GET,connector=main,view=records"`
	Write xdatly.Component[hooks.Input, hooks.Output] `component:"Write,path=/records,method=POST,connector=main,handler=hooks.NewWrite"`
}
type Input struct {
	ID int `parameter:"ID,kind=path,in=id,required"`
}
type Output struct {
	Rows []*models.Record `parameter:"Rows,kind=output,in=view" view:"records,table=records" sql:"uri=build_records:queries/read.sql" json:"rows"`
}

//go:embed queries/*.sql
var Assets embed.FS
