package bridge

import (
	"github.com/aws/aws-lambda-go/events"
	"net/http"
)

type responseWriter struct {
	proxy *events.APIGatewayProxyResponse
}

func (r *responseWriter) Header() http.Header {
	return r.proxy.MultiValueHeaders
}


func (r *responseWriter) Write(bs []byte) (int, error){
	r.proxy.Body += string(bs)
	return len(bs), nil
}

func (r *responseWriter) WriteHeader(statusCode int) {
	r.proxy.StatusCode = statusCode
}

//NewHttpWriter creates an http writer
func NewHttpWriter(proxy *events.APIGatewayProxyResponse) http.ResponseWriter {
	proxy.StatusCode = http.StatusOK
	return  &responseWriter{
		proxy:proxy,
	}
}
