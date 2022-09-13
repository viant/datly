package gateway

import "net/http"

type Authorizer interface {
	Authorize(writer http.ResponseWriter, request *http.Request) bool
}
