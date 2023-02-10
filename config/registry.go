package config

import (
	"github.com/viant/datly/plugins"
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
		TypeJwtTokenInfo: reflect.TypeOf(&JWTClaims{}),
		TypeJwtClaims:    reflect.TypeOf(JWTClaims{}),
	},
	Codecs: plugins.NewCodecs(
		plugins.NewCodec(CodecKeyJwtClaim, &GCPJwtClaim{}, reflect.TypeOf(&JWTClaims{})),
		plugins.NewCodec(CodecCognitoKeyJwtClaim, &GCPJwtClaim{}, reflect.TypeOf(&JWTClaims{})),
		plugins.NewCodec(CodecKeyAsInts, &AsInts{}, reflect.TypeOf([]int{})),
		plugins.NewCodec(CodecKeyAsStrings, &AsStrings{}, reflect.TypeOf([]string{})),
		CsvFactory(""),
		StructQLFactory(""),
	),
	Packages: map[string]map[string]reflect.Type{},
}
