package locator

import "github.com/viant/datly/view/state"

func init() {
	Register(state.KindPath, NewPath)
	Register(state.KindQuery, NewQuery)
	Register(state.KindHeader, NewHeader)
	Register(state.KindCookie, NewCookie)
	Register(state.KindRequestBody, NewBody)
	Register(state.KindEnvironment, NewEnv)

}
