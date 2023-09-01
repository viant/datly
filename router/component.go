package router

import (
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/router/content"
	"github.com/viant/datly/service/handler"
	"github.com/viant/datly/view"
)

type Component struct {
	component.Header
	View *view.View `json:",omitempty"`
	*view.NamespacedView

	Handler *handler.Handler `json:",omitempty"`

	content.Content
}
