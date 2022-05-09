package adapter

import (
	"bytes"
	"encoding/base64"
	"github.com/aws/aws-lambda-go/events"
	"io"
	"net/http"
	"strconv"
	"strings"
)

type Request events.LambdaFunctionURLRequest

func (r *Request) Request() *http.Request {
	req := http.Request{}
	req.RequestURI = r.RawPath
	req.Header = make(http.Header)
	if len(r.Headers) > 0 {
		for k, v := range r.Headers {
			req.Header[k] = []string{v}
		}
	}
	if r.Body != "" {
		var reader io.ReadCloser
		if r.IsBase64Encoded {
			if data, err := base64.StdEncoding.DecodeString(r.Body); err == nil {
				reader = io.NopCloser(bytes.NewReader(data))
				req.Header.Set("Content-Length", strconv.Itoa(len(data)))
			}
		}
		if reader == nil {
			reader = io.NopCloser(strings.NewReader(r.Body))
			req.Header.Set("Content-Length", strconv.Itoa(len(r.Body)))
		}
		req.Body = reader
	}
	req.Method = r.RequestContext.HTTP.Method

	return &req
}
