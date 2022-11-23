package adapter

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/toolbox"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Request events.APIGatewayProxyRequest

//Request converts to http.Request
//apigw doesn't include the function name in the URI segments
func (r *Request) Request(jwtSigner *signer.Service) *http.Request {
	path := r.Path
	req := http.Request{
		Method:     r.HTTPMethod,
		Header:     http.Header{},
		RequestURI: path,
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

	if ctx := r.RequestContext; len(ctx.Authorizer) > 0 {
		authorizer := ctx.Authorizer
		if req.Header.Get("Authorization") == "" && jwtSigner != nil {
			if val, ok := authorizer["userId"]; ok {
				authorizer["user_id"] = toolbox.AsInt(val)
				authorizer["userId"] = toolbox.AsInt(val)
			}
			if val, ok := authorizer["accountId"]; ok {
				authorizer["account_id"] = toolbox.AsInt(val)
				authorizer["accountId"] = toolbox.AsInt(val)
			}
			token, err := jwtSigner.Create(time.Hour, authorizer)
			if err != nil {
				log.Printf("faied to create jwtClaim: %v", err)
			}
			req.Header.Set("Authorization", token)
		}
	}

	req.RequestURI = path
	apiURI := path
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
	}
	req.Host = host
	return &req
}
