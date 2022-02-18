package bridge

import (
	"github.com/viant/datly/v0/app/aws/apigw"
	"net/http"
)

type responseWriter struct {
	proxy *apigw.ProxyResponse
}

//Header returns response headers
func (r *responseWriter) Header() http.Header {
	return r.proxy.MultiValueHeaders
}

//Write write response data
func (r *responseWriter) Write(bs []byte) (int, error) {
	r.proxy.Body += string(bs)
	return len(bs), nil
}

//WriteHeader write status code
func (r *responseWriter) WriteHeader(statusCode int) {
	r.proxy.StatusCode = statusCode
}

//NewHTTPWriter creates an http writer
func NewHTTPWriter(proxy *apigw.ProxyResponse) http.ResponseWriter {
	proxy.StatusCode = http.StatusOK
	return &responseWriter{
		proxy: proxy,
	}
}
