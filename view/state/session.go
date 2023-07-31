package state

import "github.com/viant/structology"

type (
	Session struct {
		ViewState     NamedStates
		TemplateState NamedStates
	}

	NamedStates map[string]*structology.State
)
