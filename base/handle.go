package base

import "net/http"

//Handle represents http handle
type Handle func(writer http.ResponseWriter, request *http.Request)

