package config

import (
	"encoding/json"
	"fmt"
	xcodec "github.com/viant/datly/config/codec"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/sqlx/types"
	"github.com/viant/xdatly/codec"
	_ "github.com/viant/xdatly/extension" //go mod level placeholder replacement
	"github.com/viant/xdatly/handler/response"
	"github.com/viant/xdatly/predicate"
	"github.com/viant/xdatly/types/core"
	_ "github.com/viant/xdatly/types/custom"
	"github.com/viant/xreflect"
	"reflect"
	"time"
)

const (
	TypeJwtTokenInfo        = "JwtTokenInfo"
	TypeJwtClaims           = "JwtClaims"
	CodecCognitoKeyJwtClaim = "CognitoJwtClaim"
	CodecKeyJwtClaim        = "JwtClaim"
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
		xreflect.NewType("response.Status", xreflect.WithReflectType(reflect.TypeOf(response.Status{}))),
		xreflect.NewType("predicate.StringsFilter", xreflect.WithReflectType(reflect.TypeOf(predicate.StringsFilter{}))),
		xreflect.NewType("predicate.IntFilter", xreflect.WithReflectType(reflect.TypeOf(predicate.IntFilter{}))),
		xreflect.NewType("predicate.BoolFilter", xreflect.WithReflectType(reflect.TypeOf(predicate.BoolFilter{}))),
	)),
	Codecs: codec.NewRegistry(
		codec.WithCodec(xcodec.KeyJwtClaim, &xcodec.GCPJwtClaim{}, time.Time{}),
		codec.WithCodec(xcodec.CognitoKeyJwtClaim, &xcodec.GCPJwtClaim{}, time.Time{}),
		codec.WithCodec(xcodec.KeyAsStrings, &xcodec.AsStrings{}, time.Time{}),
		codec.WithFactory(xcodec.KeyCSV, xcodec.CsvFactory(""), time.Time{}),
		codec.WithFactory(xcodec.Structql, xcodec.StructQLFactory(""), time.Time{}),
		codec.WithFactory(xcodec.JSON, &xcodec.JSONFactory{}, time.Time{}),
		codec.WithFactory(xcodec.VeltyCriteria, &xcodec.VeltyCriteriaFactory{}, time.Time{}),
		codec.WithFactory(xcodec.KeyCriteriaBuilder, &xcodec.CriteriaBuilderFactory{}, time.Time{}),
		codec.WithFactory(xcodec.Encode, &xcodec.EncodeFactory{}, time.Time{}),
		codec.WithFactory(xcodec.KeyTransfer, &xcodec.TransferFactory{}, time.Time{}),
	),
	Predicates: &PredicateRegistry{
		registry: map[string]*Predicate{
			PredicateEqual:       NewEqualPredicate(),
			PredicateNotEqual:    NewNotEqualPredicate(),
			PredicateNotIn:       NewNotInPredicate(),
			PredicateIn:          NewInPredicate(),
			PredicateMultiNotIn:  NewMultiNotInPredicate(),
			PredicateMultiIn:     NewMultiInPredicate(),
			PredicateLessOrEqual: NewLessOrEqualPredicate(),
			PredicateLike:        NewLikePredicate(),
			PredicateNotLike:     NewNotLikePredicate(),
			PredicateHandler:     NewPredicateHandler(),
			PredicateContains:    NewContainsPredicate(),
			PredicateNotContains: NewNotContainsPredicate(),
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
