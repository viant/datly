package hiddendiscovery

import (
	"github.com/viant/xdatly"
	"reflect"
)

type Component struct {
	Read    xdatly.Component[Input, Output]        `component:"Read,path=/hidden-discovery,method=GET,connector=main"`
	Unknown xdatly.Component[Input, UnknownOutput] `component:"Unknown,path=/hidden-discovery-unknown,method=GET,connector=main"`
}

var LinkedType = reflect.TypeFor[Component]()

type Input struct{}
type Row struct {
	Body         []byte  `sqlx:"binary_body" internal:"true" json:"-"`
	NullableBody *[]byte `sqlx:"nullable_body" internal:"true" json:"-"`
	ID           int     `sqlx:"id" json:"id"`
	Lease        *string `sqlx:"lease_until_raw" internal:"true" json:"-"`
	Cleanup      string  `sqlx:"cleanup_status" internal:"true" json:"-"`
	Transient    bool    `sqlx:"-" internal:"true" json:"-"`
}
type Output struct {
	Rows []*Row `json:"rows" parameter:"Rows,kind=output,in=view" view:"Rows" sql:"SELECT r.* FROM (SELECT 1 AS id, COALESCE(NULL, CAST('ab' AS BLOB)) AS binary_body, CAST(NULL AS BLOB) AS nullable_body, CAST(NULL AS CHAR) AS lease_until_raw, LOWER(TRIM(' DONE ')) AS cleanup_status) r"`
}
type UnknownOutput struct {
	Rows []*Row `json:"rows" parameter:"Rows,kind=output,in=view" view:"Rows" sql:"SELECT r.* FROM (SELECT 1 AS id, COALESCE(NULL, CAST('ab' AS BLOB)) AS binary_body, CAST(NULL AS BLOB) AS nullable_body, CAST(NULL AS CHAR) AS lease_until_raw, LOWER(TRIM(' DONE ')) AS cleanup_status, CAST(NULL AS CHAR) AS unowned) r"`
}
