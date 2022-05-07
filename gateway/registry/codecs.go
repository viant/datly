package registry

import (
	"github.com/viant/datly/auth/gcp"
	"github.com/viant/datly/visitor"
)

const (
	CodecKeyJwtClaim = "JwtClaim"
)

var Codecs = visitor.NewVisitors(
	visitor.New(CodecKeyJwtClaim, &gcp.JwtClaim{}),
)
