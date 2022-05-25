package standalone

import "net/http"

type AuthHandler interface {
	Auth(next http.HandlerFunc) http.HandlerFunc
}

type noAuth struct{}

func (a *noAuth) Auth(next http.HandlerFunc) http.HandlerFunc {
	return next
}
