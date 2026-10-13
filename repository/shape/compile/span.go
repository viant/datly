package compile

import (
	dqlpre "github.com/viant/datly/repository/shape/dql/preprocess"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
)

func relationSpan(raw string, offset int) dqlshape.Span {
	return dqlpre.PointSpan(raw, offset)
}
