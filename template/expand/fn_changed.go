package expand

import (
	"github.com/viant/godiff"
	"github.com/viant/velty/est/op"
	"github.com/viant/xreflect"
)

const fnChanged = "Changed"

var funcChanged = &op.TypeFunc{
	ResultType: xreflect.BoolType,
	Name:       fnChanged,
	Handler: func(changes *godiff.ChangeLog) bool {
		return changes.Size() != 0
	},
}
