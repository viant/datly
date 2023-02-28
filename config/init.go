package config

import (
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/xdatly/types/core"
	_ "github.com/viant/xdatly/types/custom/imports"
	"reflect"
	"time"
)

func init() {
	types, _ := core.Types(func(packageName, typeName string, rType reflect.Type, insertedAt time.Time) {
		Config.AddType(packageName, typeName, rType)
	})

	Config.OverridePackageNamedTypes(types)
}

const (
	TypeJwtTokenInfo = "JwtTokenInfo"
	TypeJwtClaims    = "JwtClaims"

	CodecCognitoKeyJwtClaim = "CognitoJwtClaim"
	CodecKeyJwtClaim        = "JwtClaim"
	CodecKeyAsStrings       = "AsStrings"
	CodecKeyAsInts          = "AsInts"
	CodecKeyCSV             = "CSV"
)

var Config = &Registry{
	Types: map[string]reflect.Type{
		TypeJwtTokenInfo: reflect.TypeOf(&jwt.Claims{}),
		TypeJwtClaims:    reflect.TypeOf(jwt.Claims{}),
	},
	Codecs: NewCodecs(
		NewCodec(CodecKeyJwtClaim, &GCPJwtClaim{}, reflect.TypeOf(&jwt.Claims{})),
		NewCodec(CodecCognitoKeyJwtClaim, &GCPJwtClaim{}, reflect.TypeOf(&jwt.Claims{})),
		NewCodec(CodecKeyAsInts, &AsInts{}, reflect.TypeOf([]int{})),
		NewCodec(CodecKeyAsStrings, &AsStrings{}, reflect.TypeOf([]string{})),
		CsvFactory(""),
		StructQLFactory(""),
	),
	Packages: map[string]map[string]reflect.Type{},
}
