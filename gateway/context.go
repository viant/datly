package gateway

import (
	"github.com/viant/datly/httputils"
	"github.com/viant/datly/template/expand"
	"net/http"
)

type (
	InterceptorContext struct {
		Request *httputils.Request `velty:"names=request"`
		Router  *RouteHandler      `velty:"names=router"`
	}

	IntereceptorState struct {
		ExpandState *expand.State
		Context     *InterceptorContext
	}
	RouteHandler struct {
		request    *http.Request `velty:"-"`
		redirected bool          `velty:"-"`
	}
)

func (r *RouteHandler) RedirectTo(path string) string {
	r.request.URL.Path = path
	r.redirected = true
	return ""
}
