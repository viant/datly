package translator

import (
	"github.com/viant/datly/internal/inference"
	"github.com/viant/datly/view"
)

type (
	View struct {
		view.View
		Spec *inference.Spec
	}
)
