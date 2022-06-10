package registry

import (
	"github.com/viant/datly/auth/gcp"
	"github.com/viant/datly/codec"
)

const (
	CodecKeyJwtClaim  = "JwtClaim"
	CodecKeyAsStrings = "AsStrings"
)

var Codecs = codec.NewVisitors(
	codec.New(CodecKeyJwtClaim, &gcp.JwtClaim{}),
	codec.New(CodecKeyAsStrings, &AsStrings{}),
)
