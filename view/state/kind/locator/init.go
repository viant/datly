package locator

import "github.com/viant/datly/view/state"

func init() {
	Register(state.KindRequest, NewHttpRequest)
	Register(state.KindPath, NewPath)
	Register(state.KindQuery, NewQuery)
	Register(state.KindForm, NewForm)
	Register(state.KindView, NewView)
	Register(state.KindHeader, NewHeader)
	Register(state.KindCookie, NewCookie)
	Register(state.KindRequestBody, NewBody)
	Register(state.KindEnvironment, NewEnv)
	Register(state.KindParam, NewParameter)
	Register(state.KindObject, NewObject)
	Register(state.KindRepeated, NewRepeated)
	Register(state.KindState, NewState)
	Register(state.KindContext, NewContext)
	Register(state.KindGenerator, NewGenerator)
	Register(state.KindTransient, NewTransient)
	Register(state.KindConst, NewConstants)

}
