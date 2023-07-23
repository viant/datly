package config

import (
	"encoding/json"
	"fmt"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/sqlx/types"
	_ "github.com/viant/xdatly/extension" //go mod level placeholder replacement
	"github.com/viant/xdatly/types/core"
	_ "github.com/viant/xdatly/types/custom"
	"github.com/viant/xreflect"
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
	CodecJSON               = "JSON"
)

var Config = &Registry{
	Types: xreflect.NewTypes(xreflect.WithTypes(
		xreflect.NewType(TypeJwtTokenInfo, xreflect.WithReflectType(reflect.TypeOf(&jwt.Claims{}))),
		xreflect.NewType(TypeJwtClaims, xreflect.WithReflectType(reflect.TypeOf(jwt.Claims{}))),
		xreflect.NewType("RawMessage", xreflect.WithReflectType(reflect.TypeOf(json.RawMessage{}))),
		xreflect.NewType("json.RawMessage", xreflect.WithReflectType(reflect.TypeOf(json.RawMessage{}))),
		xreflect.NewType("json.RawMessage", xreflect.WithReflectType(reflect.TypeOf(json.RawMessage{}))),
		xreflect.NewType("types.BitBool", xreflect.WithReflectType(reflect.TypeOf(types.BitBool(true)))),
	)),
	Codecs: NewCodecs(
		NewCodec(CodecKeyJwtClaim, &GCPJwtClaim{}, reflect.TypeOf(&jwt.Claims{})),
		NewCodec(CodecCognitoKeyJwtClaim, &GCPJwtClaim{}, reflect.TypeOf(&jwt.Claims{})),
		NewCodec(CodecKeyAsInts, &AsInts{}, reflect.TypeOf([]int{})),
		NewCodec(CodecKeyAsStrings, &AsStrings{}, reflect.TypeOf([]string{})),
		CsvFactory(""),
		StructQLFactory(""),
		&JSONFactory{},
		&VeltyCriteriaFactory{},
		&CriteriaBuilderFactory{},
	),
	Predicates: PredicateRegistry{
		PredicateEqual:    NewEqualPredicate(),
		PredicateNotEqual: NewNotEqualPredicate(),
		PredicateNotIn:    NewNotInPredicate(),
		PredicateIn:       NewInPredicate(),
	},
}

func init() {
	types, _ := core.Types(nil)
	for packageName, typesRegsitry := range types {
		for typeName, rType := range typesRegsitry {
			err := Config.Types.Register(typeName, xreflect.WithPackage(packageName), xreflect.WithReflectType(rType))
			if err != nil {
				fmt.Printf("[ERROR] %v \n", err.Error())
			}
		}
	}
}
