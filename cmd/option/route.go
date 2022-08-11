package option

import "github.com/viant/datly/view"

type Route struct {
	URI       string
	URIParams map[string]bool
	Cache     *view.Cache
	Method    string
}
