package registry

import (
	"github.com/viant/scy/auth/jwt"
	"reflect"
)

const (
	TypeJwtTokenInfo = "JwtTokenInfo"
)

var Types = map[string]reflect.Type{
	TypeJwtTokenInfo: reflect.TypeOf(&jwt.Claims{}),
}
