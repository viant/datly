package marshal

import "github.com/viant/datly/template/expand"

type State struct {
	Ctx         CustomContext
	ExpandState *expand.State
}
