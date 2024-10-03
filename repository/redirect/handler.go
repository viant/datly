package redirect

import "github.com/viant/datly/view/state"

type Handler struct {
	URL   string
	Input *state.Parameters
}
