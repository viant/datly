package registry

import (
	"github.com/viant/datly/auth/gcp"
	"github.com/viant/datly/visitor"
)

const (
	VisitorKeyIdJwtTokenInfo = "IdJwtTokenInfo"
)

var Visitors = visitor.NewVisitors(
	visitor.New(VisitorKeyIdJwtTokenInfo, &gcp.IdJwtTokenInfo{}),
)
