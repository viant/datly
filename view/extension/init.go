package extension

import (
	"encoding/json"
	"fmt"
	codec3 "github.com/viant/datly/view/extension/codec"
	"github.com/viant/datly/view/extension/codec/jsontab"
	"github.com/viant/datly/view/extension/codec/xmlfilter"
	"github.com/viant/datly/view/extension/codec/xmltab"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/sqlx/types"
	"github.com/viant/xdatly/codec"
	_ "github.com/viant/xdatly/extension" //go mod level placeholder replacement
	"github.com/viant/xdatly/handler/async"
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
		xreflect.NewType("response.JobInfo", xreflect.WithReflectType(reflect.TypeOf(response.JobInfo{}))),

		xreflect.NewType("predicate.StringsFilter", xreflect.WithReflectType(reflect.TypeOf(predicate.StringsFilter{}))),
		xreflect.NewType("predicate.IntFilter", xreflect.WithReflectType(reflect.TypeOf(predicate.IntFilter{}))),
		xreflect.NewType("predicate.BoolFilter", xreflect.WithReflectType(reflect.TypeOf(predicate.BoolFilter{}))),
		xreflect.NewType("xmltab.Result", xreflect.WithReflectType(reflect.TypeOf(xmltab.Result{}))),
		xreflect.NewType("xmlfilter.Result", xreflect.WithReflectType(reflect.TypeOf(xmlfilter.Result{}))),
		xreflect.NewType("jsontab.Result", xreflect.WithReflectType(reflect.TypeOf(jsontab.Result{}))),
		xreflect.NewType("async.Job", xreflect.WithReflectType(reflect.TypeOf(async.Job{}))),
		xreflect.NewType("predicate.NamedFilters", xreflect.WithReflectType(reflect.TypeOf(predicate.NamedFilters{}))),
	)),
	Codecs: codec.NewRegistry(
		codec.WithCodec(codec3.KeyJwtClaim, &codec3.GCPJwtClaim{}, time.Time{}),
		codec.WithCodec(codec3.CognitoKeyJwtClaim, &codec3.GCPJwtClaim{}, time.Time{}),
		codec.WithCodec(codec3.KeyAsStrings, &codec3.AsStrings{}, time.Time{}),
		codec.WithFactory(codec3.KeyCSV, codec3.CsvFactory(""), time.Time{}),
		codec.WithFactory(codec3.Structql, codec3.StructQLFactory(""), time.Time{}),
		codec.WithFactory(codec3.JSON, &codec3.JSONFactory{}, time.Time{}),
		codec.WithFactory(codec3.VeltyCriteria, &codec3.VeltyCriteriaFactory{}, time.Time{}),
		codec.WithFactory(codec3.KeyCriteriaBuilder, &codec3.CriteriaBuilderFactory{}, time.Time{}),
		codec.WithFactory(codec3.Encode, &codec3.EncodeFactory{}, time.Time{}),
		codec.WithFactory(codec3.KeyTransfer, &codec3.TransferFactory{}, time.Time{}),
		codec.WithFactory(codec3.KeyXmlTab, &codec3.XmlTabFactory{}, time.Time{}),
		codec.WithFactory(codec3.KeyXmlFilter, &codec3.XmlFilterFactory{}, time.Time{}),
		codec.WithFactory(codec3.KeyJsonTab, &codec3.JsonTabFactory{}, time.Time{}),
		codec.WithFactory(codec3.KeyFilters, &codec3.FiltersRegistry{}, time.Time{}),
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
