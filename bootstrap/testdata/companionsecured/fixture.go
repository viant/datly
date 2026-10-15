package companionsecured

import (
	"context"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/xdatly"
	legacy "github.com/viant/xdatly/handler/response"
)

type Input struct {
	JWT  *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
	ID   int         `parameter:"ID,kind=query,in=id,required=false"`
	Auth *AuthOutput `parameter:"Auth,kind=component,in=GET:/companion-auth"`
	Has  *InputHas   `setMarker:"true" json:"-"`
}
type InputHas struct{ JWT, ID, Auth bool }
type AuthInput struct {
	JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required=true,errorCode=401" codec:"JwtClaim"`
}
type AuthOutput struct{ Allowed bool }
type Output struct {
	legacy.Status `parameter:"Status,kind=output,in=status" json:",omitempty"`
	Data          []*Row `parameter:"Data,kind=output,in=view" view:"secured,type=Row,limit=2" sql:"SELECT id,label FROM records" json:"data"`
}
type Row struct {
	ID    int    `sqlx:"id" json:"id"`
	Label string `sqlx:"label" json:"label"`
}
type selectors struct {
	Fields []string `parameter:"Fields,kind=query,in=fields,required=false" querySelector:"view=secured,property=fields"`
	Page   int      `parameter:"Page,kind=query,in=page,required=false" querySelector:"view=secured,property=page"`
}
type Component struct {
	Contract xdatly.Component[Input, Output] `component:"SecuredRecords,path=/companion-secured,method=GET"`
}

func (Component) DatlyQuerySelectors() selectors { panic("declaration method must not execute") }

var Inputs chan *Input

func (i *Input) Init(context.Context) error {
	if Inputs != nil {
		Inputs <- i
	}
	return nil
}

var Linked = new(Component)
