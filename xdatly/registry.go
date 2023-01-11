package xdatly

import (
	"reflect"
	"sync"
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

type Registry struct {
	sync.Mutex
	Types  map[string]reflect.Type
	Codecs CodecsRegistry
}

var Config = &Registry{
	Types: map[string]reflect.Type{
		TypeJwtTokenInfo: reflect.TypeOf(&JWTClaims{}),
		TypeJwtClaims:    reflect.TypeOf(JWTClaims{}),
	},
	Codecs: NewCodecs(
		NewCodec(CodecKeyJwtClaim, &GCPJwtClaim{}, reflect.TypeOf(&JWTClaims{})),
		NewCodec(CodecCognitoKeyJwtClaim, &GCPJwtClaim{}, reflect.TypeOf(&JWTClaims{})),
		NewCodec(CodecKeyAsInts, &AsInts{}, reflect.TypeOf([]int{})),
		NewCodec(CodecKeyAsStrings, &AsStrings{}, reflect.TypeOf([]string{})),
		CsvFactory(""),
		StructQLFactory(""),
	),
}

func (r *Registry) LookupCodec(name string) (BasicCodec, error) {
	return r.Codecs.Lookup(name)
}

func (r *Registry) RegisterCodec(visitor BasicCodec) {
	r.Codecs.Register(visitor)
}

func (r *Registry) Override(toOverride *Registry) {
	r.Lock()
	defer r.Unlock()

	for typeName, rType := range toOverride.Types {
		r.Types[typeName] = rType
	}

	for _, codec := range toOverride.Codecs {
		r.Codecs.Register(codec)
	}
}
