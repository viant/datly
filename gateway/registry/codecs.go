package registry

import (
	"github.com/viant/datly/auth/gcp"
	"github.com/viant/datly/visitor"
)

const (
	CodecKeyIdJwtTokenInfo = "IdJwtTokenInfo"
)

var Codecs = visitor.NewVisitors(
	visitor.New(CodecKeyIdJwtTokenInfo, &gcp.IdJwtTokenInfo{}),
)
