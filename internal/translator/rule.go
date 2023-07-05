package translator

import "github.com/viant/datly/router"

type (
	Rule struct {
		Route *router.Route
		Namespaces
		RootNamespace string
	}

	Function struct {
		Name string
		Args []string
	}
	Namespaces map[string]Namespace

	Namespace struct {
		Name       string
		Exclude    []string
		Transforms map[string]*Function
		View       *View
	}
)
