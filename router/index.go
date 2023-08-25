package router

import (
	"github.com/viant/datly/view"
)

type (
	ViewDetails struct {
		MainView bool
		View     *view.View
		Path     string
		Prefixes []string
	}
)
