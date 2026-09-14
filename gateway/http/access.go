package http

import (
	stdhttp "net/http"
	"strings"
)

// allowsRemote follows original gateway/runtime/meta.IsAuthorized: configured
// prefixes apply to the server's RemoteAddr before every dispatch. Forwarding
// headers do not establish peer authority and this creates no principal.
func (h *Handler) allowsRemote(request *stdhttp.Request) bool {
	if len(h.allowedSubnet) == 0 {
		return true
	}
	for _, prefix := range h.allowedSubnet {
		if strings.HasPrefix(request.RemoteAddr, prefix) {
			return true
		}
	}
	return false
}
