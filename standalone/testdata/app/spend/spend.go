package spend

import (
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/x"
	xdatly "github.com/viant/xdatly"
	"reflect"
)

type Component struct {
	Spend     xdatly.Component[Input, Output]     `component:"Spend,path=/spend,method=GET,connector=main,view=spend,report=true,reportCompose=true" mcp:"[{\"kind\":\"tool\",\"name\":\"Spend\"}]"`
	Protected xdatly.Component[AuthInput, Output] `component:"Protected,path=/secure,method=GET,connector=main,view=spend,report=true,reportCompose=true,reportMCPTool=false,reportComposeMCPTool=false" apiKeyHeader:"X-Report-Key" apiKeyValue:"report-secret"`
}

type Access struct {
	Tenant string `sqlx:"tenant"`
}

type FilterFields struct {
	Tenant  string  `parameter:"Tenant,kind=header,in=X-Tenant,required" predicate:"equal,s,tenant"`
	Channel *string `parameter:"Channel,kind=cookie,in=channel" predicate:"equal,s,channel"`
}
type Input struct {
	FilterFields
	Allowed []*Access `parameter:"Allowed,kind=view,in=Allowed,required" view:"Allowed,table=report_access" sql:"SELECT tenant FROM report_access WHERE tenant=:Tenant"`
}
type AuthInput struct {
	FilterFields
	Allowed []*Access   `parameter:"Allowed,kind=view,in=Allowed,required" view:"Allowed,table=report_access" sql:"SELECT tenant FROM report_access WHERE tenant=:Tenant"`
	JWT     *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" json:"-"`
}

type Row struct {
	AccountID  int     `sqlx:"customer_id" json:"accountID" groupable:"true"`
	Region     string  `sqlx:"region_code" json:"region" groupable:"true"`
	TotalSpend float64 `sqlx:"spend_total" json:"totalSpend"`
	OrderCount int     `sqlx:"order_count" json:"orderCount"`
}

type Output struct {
	Rows []*Row `parameter:"Rows,kind=output,in=view" view:"spend,groupable=true,selectorProjection=true,selectorOrderBy=true,selectorLimit=true,selectorOffset=true" sql:"uri=queries/spend.sql" json:"rows"`
}

func Exports() *x.Registry {
	r := x.NewRegistry()
	for _, typ := range []reflect.Type{reflect.TypeFor[Access](), reflect.TypeFor[Input](), reflect.TypeFor[AuthInput](), reflect.TypeFor[Output](), reflect.TypeFor[Row](), reflect.TypeFor[jwt.Claims]()} {
		r.Register(x.NewType(typ))
	}
	return r
}
