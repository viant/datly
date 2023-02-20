package config

import (
	"github.com/viant/datly/plugins"
	"github.com/viant/scy/auth/jwt"
	"reflect"
)

const (
	TypeJwtTokenInfo = "JwtTokenInfo"
	TypeJwtClaims    = "JwtClaims"

	CodecCognitoKeyJwtClaim = "CognitoJwtClaim"
	CodecKeyJwtClaim        = "JwtClaim"
	CodecKeyAsStrings       = "AsStrings"
	CodecKeyAsInts          = "AsInts"
	CodecKeyCSV             = "CSV"
)

var Config = &plugins.Registry{
	Types: map[string]reflect.Type{
		TypeJwtTokenInfo: reflect.TypeOf(&jwt.Claims{}),
		TypeJwtClaims:    reflect.TypeOf(jwt.Claims{}),
	},
	Codecs: plugins.NewCodecs(
		plugins.NewCodec(CodecKeyJwtClaim, &GCPJwtClaim{}, reflect.TypeOf(&jwt.Claims{})),
		plugins.NewCodec(CodecCognitoKeyJwtClaim, &GCPJwtClaim{}, reflect.TypeOf(&jwt.Claims{})),
		plugins.NewCodec(CodecKeyAsInts, &AsInts{}, reflect.TypeOf([]int{})),
		plugins.NewCodec(CodecKeyAsStrings, &AsStrings{}, reflect.TypeOf([]string{})),
		CsvFactory(""),
		StructQLFactory(""),
	),
	Packages: map[string]map[string]reflect.Type{},
}
