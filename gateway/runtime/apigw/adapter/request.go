package adapter

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Request events.APIGatewayProxyRequest

func (r *Request) Request() *http.Request {
	req := http.Request{
		Method:     r.HTTPMethod,
		Header:     http.Header{},
		RequestURI: r.Path,
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
	apiURI := r.Path
	if len(r.MultiValueQueryStringParameters) > 0 {
		values := url.Values(r.MultiValueQueryStringParameters)
		apiURI += "?" + values.Encode()
	}
	if strings.HasPrefix(apiURI, "/") {
		apiURI = apiURI[1:]
	}
	host := req.Header.Get("Host")
	if host == "" {
		if host = r.RequestContext.DomainName; host == "" {
			host = "localhost"
		}
	}
	URL, err := url.Parse(fmt.Sprintf("https://%v/%v", host, apiURI))
	if err == nil {
		req.URL = URL
		req.RequestURI = URL.RawPath
	}
	req.Host = host
	return &req
}
