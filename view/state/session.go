package state

import "github.com/viant/structology"

type (
	Session struct {
		State         *structology.State //resource state
		ViewState     NamedStates
		TemplateState NamedStates
	}

	NamedStates map[string]*structology.State
)
