package marshal

import (
	"github.com/viant/datly/service/executor/expand"
)

type State struct {
	Ctx         CustomContext
	ExpandState *expand.State
}
