package authrecords

import (
	"reflect"

	"github.com/viant/scy/auth/jwt"
	"github.com/viant/x"
	xdatly "github.com/viant/xdatly"
)

type Component struct {
	Protected xdatly.Component[Input, Output] `component:"Protected,path=/protected/{id},method=GET,connector=main,view=records"`
}
type Input struct {
	ID  int         `parameter:"ID,kind=path,in=id,required"`
	JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" json:"-"`
}
type Record struct {
	ID   int    `sqlx:"id" json:"id"`
	Name string `sqlx:"name" json:"name"`
}
type Output struct {
	Rows []*Record `parameter:"Rows,kind=output,in=view" view:"records,table=records" sql:"SELECT id,name FROM records WHERE id=:ID" json:"rows"`
}

func Exports() *x.Registry {
	r := x.NewRegistry()
	for _, typ := range []reflect.Type{reflect.TypeFor[Input](), reflect.TypeFor[Output](), reflect.TypeFor[Record](), reflect.TypeFor[jwt.Claims]()} {
		r.Register(x.NewType(typ))
	}
	return r
}
