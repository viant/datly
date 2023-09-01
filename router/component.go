package router

import (
	"github.com/viant/datly/router/component"
	"github.com/viant/datly/router/content"
	"github.com/viant/datly/view"
)

type Component struct {
	Path
	component.Contract

	View *view.View `json:",omitempty"`
	*view.NamespacedView

	Handler *Handler `json:",omitempty"`

	content.Content
}
