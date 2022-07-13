package registry

import (
	"github.com/viant/datly/auth/gcp"
	"github.com/viant/datly/codec"
	"github.com/viant/scy/auth/jwt"
	"reflect"
)

const (
	CodecKeyJwtClaim  = "JwtClaim"
	CodecKeyAsStrings = "AsStrings"
)

var Codecs = codec.NewVisitors(
	codec.NewCodec(CodecKeyJwtClaim, &gcp.JwtClaim{}, reflect.TypeOf(&jwt.Claims{})),
	codec.NewCodec(CodecKeyAsStrings, &AsStrings{}, reflect.TypeOf([]string{})),
)
