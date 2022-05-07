package registry

import (
	"google.golang.org/api/oauth2/v2"
	"reflect"
)

const (
	TypeJwtTokenInfo = "JwtTokenInfo"
)

var Types = map[string]reflect.Type{
	TypeJwtTokenInfo: reflect.TypeOf(&oauth2.Tokeninfo{}),
}
