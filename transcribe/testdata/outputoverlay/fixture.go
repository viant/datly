package outputoverlay

import (
	"embed"
	"github.com/viant/xdatly/handler/response"
)

//go:embed sql/*.sql
var Resources embed.FS

type Input struct {
	ID  int       `parameter:"ID,kind=query,in=id,dataType=int,required=false" predicate:"equal,r,ID"`
	Has *InputHas `setMarker:"true" json:"-"`
}
type InputHas struct{ ID bool }

func (*Input) EmbedFS() *embed.FS { return &Resources }

type Output struct {
	response.Status `parameter:"Status,kind=output,in=status" json:",omitempty"`
	Data            []*Row `parameter:"Data,kind=output,in=view" view:"Records,type=Row,table=records,selectorProjection=true,selectorNoLimit=true" sql:"uri=sql/records.sql"`
}
type Row struct {
	ID     int     `sqlx:"ID"`
	Name   *string `sqlx:"NAME"`
	Metric *Metric `view:"metric,type=Metric,table=metrics,selectorNoLimit=true" on:"ID=ID" sql:"uri=sql/metric.sql"`
}
type Metric struct {
	ID     int      `sqlx:"ID"`
	Amount *float64 `sqlx:"AMOUNT"`
}
type selectors struct {
	Fields []string `parameter:"Fields,kind=query,in=fields,dataType=[]string,required=false,cacheable=false" querySelector:"view=Records,property=fields"`
}
