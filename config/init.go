package config

import (
	"encoding/json"
	"fmt"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/sqlx/types"
	"github.com/viant/xdatly/codec"
	_ "github.com/viant/xdatly/extension" //go mod level placeholder replacement
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xdatly/types/core"
	_ "github.com/viant/xdatly/types/custom"
	"github.com/viant/xreflect"
	"reflect"
	"time"
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
		xreflect.NewType("time.Time", xreflect.WithReflectType(xreflect.TimeType)),
	)),
	Codecs: codec.NewRegistry(
		codec.WithCodec(CodecKeyJwtClaim, &GCPJwtClaim{}, time.Time{}),
		codec.WithCodec(CodecCognitoKeyJwtClaim, &GCPJwtClaim{}, time.Time{}),
		codec.WithCodec(CodecKeyAsStrings, &AsStrings{}, time.Time{}),
		codec.WithFactory(CodecKeyCSV, CsvFactory(""), time.Time{}),
		codec.WithFactory(CodecStructql, StructQLFactory(""), time.Time{}),
		codec.WithFactory(CodecJSON, &JSONFactory{}, time.Time{}),
		codec.WithFactory(CodecVeltyCriteria, &VeltyCriteriaFactory{}, time.Time{}),
		codec.WithFactory(CodecCriteriaBuilder, &CriteriaBuilderFactory{}, time.Time{}),
		codec.WithFactory(CodecEncode, &EncodeFactory{}, time.Time{}),
	),
	Predicates: &PredicateRegistry{
		registry: map[string]*predicate.Template{
			PredicateEqual:       NewEqualPredicate(),
			PredicateNotEqual:    NewNotEqualPredicate(),
			PredicateNotIn:       NewNotInPredicate(),
			PredicateIn:          NewInPredicate(),
			PredicateMultiNotIn:  NewMultiNotInPredicate(),
			PredicateMultiIn:     NewMultiInPredicate(),
			PredicateLessOrEqual: NewLessOrEqualPredicate(),
			PredicateLike:        NewLikePredicate(),
			PredicateNotLike:     NewNotLikePredicate(),
		},
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

	predicates, _ := predicate.Templates(nil)
	for _, template := range predicates {
		Config.Predicates.Add(template)
	}

	codecs, _ := codec.Codecs(nil)
	for key, value := range codecs {
		Config.Codecs.RegisterCodec(key, value)
	}
}
