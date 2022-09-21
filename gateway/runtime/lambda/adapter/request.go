package adapter

import (
	"bytes"
	"encoding/base64"
	"github.com/aws/aws-lambda-go/events"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Request events.LambdaFunctionURLRequest

func (r *Request) Request() *http.Request {
	req := http.Request{
		Method:     r.RequestContext.HTTP.Method,
		Header:     http.Header{},
		RequestURI: r.RawPath,
	}
	if len(r.Headers) > 0 {
		for k, v := range r.Headers {
			req.Header.Set(k, v)
		}
	}
	if r.Body != "" {
		if r.IsBase64Encoded {
			if data, err := base64.StdEncoding.DecodeString(r.Body); err == nil {
				req.Body = io.NopCloser(bytes.NewReader(data))
				req.Header.Set("content-length", strconv.Itoa(len(data)))
			}
		}

		if req.Body == nil {
			req.Body = io.NopCloser(strings.NewReader(r.Body))
			req.Header.Set("Content-Length", strconv.Itoa(len(r.Body)))
		}
	}

	host := req.Header.Get("Host")
	if host == "" {
		host = r.RequestContext.DomainName
	}
	req.Host = host
	URI := r.RawPath
	if URI != "" && URI[0] != '/' {
		URI = "/" + URI
	}

	if r.RawQueryString != "" {
		URI += "?" + r.RawQueryString
	}
	req.URL, _ = url.Parse("https://" + host + URI)
	return &req
}
