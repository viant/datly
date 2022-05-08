package meta

import (
	"net/http"
	"strings"
)

//IsAuthorized checks if request is authorized
func IsAuthorized(request *http.Request, allowedSubset []string) bool {
	if len(allowedSubset) == 0 {
		return true
	}
	for _, allowed := range allowedSubset {
		if strings.HasPrefix(request.RemoteAddr, allowed) {
			return true
		}
	}
	return false
}
