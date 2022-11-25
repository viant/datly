package registry

import (
	"fmt"
	"github.com/viant/datly/auth/gcp"
	"github.com/viant/datly/view"
	"github.com/viant/scy/auth/jwt"
	"reflect"
)

const (
	CodecCognitoKeyJwtClaim = "CognitoJwtClaim"
	CodecKeyJwtClaim        = "JwtClaim"
	CodecKeyAsStrings       = "AsStrings"
	CodecKeyAsInts          = "AsInts"
	CodecKeyCSV             = "CSV"
)

var Codecs = view.NewCodecs(
	view.NewCodec(CodecKeyJwtClaim, &gcp.JwtClaim{}, reflect.TypeOf(&jwt.Claims{})),
	view.NewCodec(CodecCognitoKeyJwtClaim, &gcp.JwtClaim{}, reflect.TypeOf(&jwt.Claims{})),
	view.NewCodec(CodecKeyAsInts, &AsInts{}, reflect.TypeOf([]int{})),
	view.NewCodec(CodecKeyAsStrings, &AsStrings{}, reflect.TypeOf([]string{})),
	CsvFactory(""),
	StructQLFactory(""),
)

func unexpectedUseError(on interface{}) error {
	return fmt.Errorf("unexpected use Value on %T", on)
}
