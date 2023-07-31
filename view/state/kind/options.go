package kind

import "net/http"

type options struct {
	requests *http.Request
}

type Option func(o *options)
