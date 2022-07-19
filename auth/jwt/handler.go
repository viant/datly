package jwt

import "net/http"

type Authenticator interface {
	Auth(next http.HandlerFunc) http.HandlerFunc
}
