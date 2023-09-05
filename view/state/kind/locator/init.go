package locator

import "github.com/viant/datly/view/state"

func init() {
	Register(state.KindRequest, NewHttpRequest)
	Register(state.KindPath, NewPath)
	Register(state.KindQuery, NewQuery)
	Register(state.KindDataView, NewDataView)
	Register(state.KindHeader, NewHeader)
	Register(state.KindCookie, NewCookie)
	Register(state.KindRequestBody, NewBody)
	Register(state.KindEnvironment, NewEnv)
	Register(state.KindParam, NewParameter)
	Register(state.KindGroup, NewGroup)
	Register(state.KindRepeated, NewRepeated)
	Register(state.KindState, NewState)
}
