package extension

import (
	"encoding/json"
	"fmt"
	dcodec "github.com/viant/datly/view/extension/codec"
	"github.com/viant/datly/view/extension/handler"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/sqlx/types"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/docs"
	_ "github.com/viant/xdatly/extension" //go mod level placeholder replacement
	"github.com/viant/xdatly/handler/async"
	"github.com/viant/xdatly/handler/response"
	"github.com/viant/xdatly/handler/response/tabular/tjson"
	"github.com/viant/xdatly/handler/response/tabular/xml"

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

var Config *Registry

func init() {
	InitRegistry()
}

func InitRegistry() {
	Config = &Registry{
		Types: xreflect.NewTypes(xreflect.WithTypes(
			xreflect.NewType(TypeJwtTokenInfo, xreflect.WithReflectType(reflect.TypeOf(&jwt.Claims{}))),
			xreflect.NewType(TypeJwtClaims, xreflect.WithReflectType(reflect.TypeOf(jwt.Claims{}))),
			xreflect.NewType("jwt.Claims", xreflect.WithReflectType(reflect.TypeOf(jwt.Claims{}))),
			xreflect.NewType("RawMessage", xreflect.WithReflectType(reflect.TypeOf(json.RawMessage{}))),
			xreflect.NewType("json.RawMessage", xreflect.WithReflectType(reflect.TypeOf(json.RawMessage{}))),
			xreflect.NewType("json.RawMessage", xreflect.WithReflectType(reflect.TypeOf(json.RawMessage{}))),
			xreflect.NewType("types.BitBool", xreflect.WithReflectType(reflect.TypeOf(types.BitBool(true)))),
			xreflect.NewType("time.Time", xreflect.WithReflectType(xreflect.TimeType)),
			xreflect.NewType("response.Status", xreflect.WithReflectType(reflect.TypeOf(response.Status{}))),
			xreflect.NewType("response.Metrics", xreflect.WithReflectType(reflect.TypeOf(response.Metrics{}))),
			xreflect.NewType("response.Metric", xreflect.WithReflectType(reflect.TypeOf(response.Metric{}))),
			xreflect.NewType("response.JobInfo", xreflect.WithReflectType(reflect.TypeOf(response.JobInfo{}))),

			xreflect.NewType("predicate.StringsFilter", xreflect.WithReflectType(reflect.TypeOf(predicate.StringsFilter{}))),
			xreflect.NewType("predicate.IntFilter", xreflect.WithReflectType(reflect.TypeOf(predicate.IntFilter{}))),
			xreflect.NewType("predicate.BoolFilter", xreflect.WithReflectType(reflect.TypeOf(predicate.BoolFilter{}))),
			xreflect.NewType("xmltab.Result", xreflect.WithReflectType(reflect.TypeOf(xml.Tabular{}))),
			xreflect.NewType("xmlfilter.Result", xreflect.WithReflectType(reflect.TypeOf(xml.FilterHolder{}))),
			xreflect.NewType("jsontab.Result", xreflect.WithReflectType(reflect.TypeOf(tjson.Tabular{}))),

			xreflect.NewType("xml.Tabular", xreflect.WithReflectType(reflect.TypeOf(xml.Tabular{}))),
			xreflect.NewType("xml.FilterHolder", xreflect.WithReflectType(reflect.TypeOf(xml.FilterHolder{}))),
			xreflect.NewType("tjson.Tabular", xreflect.WithReflectType(reflect.TypeOf(tjson.Tabular{}))),

			xreflect.NewType("async.Job", xreflect.WithReflectType(reflect.TypeOf(async.Job{}))),
			xreflect.NewType("predicate.NamedFilters", xreflect.WithReflectType(reflect.TypeOf(predicate.NamedFilters{}))),
			xreflect.NewType("LoadData", xreflect.WithReflectType(reflect.TypeOf(&handler.LoadDataProvider{}))),
		)),
		Codecs: codec.New(
			codec.WithCodec(dcodec.KeyJwtClaim, &dcodec.GCPJwtClaim{}, time.Time{}),
			codec.WithCodec(dcodec.CognitoKeyJwtClaim, &dcodec.GCPJwtClaim{}, time.Time{}),
			codec.WithCodec(dcodec.KeyAsStrings, &dcodec.AsStrings{}, time.Time{}),
			codec.WithCodec(dcodec.KeyAsInts, &dcodec.AsInts{}, time.Time{}),
			codec.WithCodec(dcodec.KeyNil, &dcodec.Nil{}, time.Time{}),
			codec.WithCodec(dcodec.Structql, &dcodec.StructQLCodec{}, time.Time{}),
			codec.WithFactory(dcodec.KeyCSV, dcodec.CsvFactory(""), time.Time{}),
			codec.WithFactory(dcodec.Structql, dcodec.StructQLFactory(""), time.Time{}),
			codec.WithFactory(dcodec.JSON, &dcodec.JSONFactory{}, time.Time{}),
			codec.WithFactory(dcodec.VeltyCriteria, &dcodec.VeltyCriteriaFactory{}, time.Time{}),
			codec.WithFactory(dcodec.KeyCriteriaBuilder, &dcodec.CriteriaBuilderFactory{}, time.Time{}),
			codec.WithFactory(dcodec.Encode, &dcodec.EncodeFactory{}, time.Time{}),
			codec.WithFactory(dcodec.KeyTransfer, &dcodec.TransferFactory{}, time.Time{}),
			codec.WithFactory(dcodec.KeyXmlTab, &dcodec.XmlTabFactory{}, time.Time{}),
			codec.WithFactory(dcodec.KeyXmlFilter, &dcodec.XmlFilterFactory{}, time.Time{}),
			codec.WithFactory(dcodec.KeyJsonTab, &dcodec.JsonTabFactory{}, time.Time{}),
			codec.WithFactory(dcodec.KeyFilters, &dcodec.FiltersRegistry{}, time.Time{}),
			codec.WithFactory(dcodec.KeyURIRewrite, &dcodec.URIRewriterFactory{}, time.Time{}),
			codec.WithFactory(dcodec.KeyURIChecksum, &dcodec.UriChecksumFactory{}, time.Time{}),
			codec.WithFactory(dcodec.KeyTimeDiff, &dcodec.TimeDiffFactory{}, time.Time{}),
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
		Docs: docs.New(),
	}
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

	docs.ForEach(func(name string, provider docs.Provider) {
		Config.Docs.Register(name, provider)
	})
}
