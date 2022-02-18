package bridge

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/viant/datly/v0/app/aws/apigw"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

//NewHTTPRequest create a request for supplied apiRequest
func NewHTTPRequest(apiRequest *apigw.ProxyRequest) (*http.Request, error) {
	request := &http.Request{
		Method: apiRequest.HTTPMethod,
		Header: http.Header{},
	}
	apiURI := apiRequest.Path
	if len(apiRequest.MultiValueQueryStringParameters) > 0 {
		values := url.Values(apiRequest.MultiValueQueryStringParameters)
		apiURI += "?" + values.Encode()
	}
	if !strings.HasPrefix(apiURI, "/") {
		apiURI = "/" + apiURI
	}
	URL, err := url.Parse(fmt.Sprintf("https://localhost%v", apiURI))
	if err != nil {
		return nil, err
	}
	request.URL = URL
	if URL != nil {
		request.RequestURI = URL.RawPath
	}
	if apiRequest.Body != "" {
		if apiRequest.IsBase64Encoded {
			payload, err := base64.StdEncoding.DecodeString(apiRequest.Body)
			if err != nil {
				return nil, err
			}
			request.Body = ioutil.NopCloser(bytes.NewReader(payload))
		} else {
			request.Body = ioutil.NopCloser(strings.NewReader(apiRequest.Body))
		}
	}
	request.Header = apiRequest.MultiValueHeaders
	return request, nil
}
