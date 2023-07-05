package translator

import "github.com/viant/datly/router"

type (
	Rule struct {
		Route *router.Route
		Namespaces
		RootNamespace string
	}
)
