package translator

import (
	"github.com/envoyproxy/go-control-plane/envoy/api/v2/route"
)

type (
	Rule struct {
		Route *route.Route
		Namespaces
	}

	Namespaces map[string]Namespace
	Namespace  struct {
		Name    string
		Exclude []string
	}
)
