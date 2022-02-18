package lambda

import (
	"context"
	"github.com/viant/afs/option"
	apigw2 "github.com/viant/datly/v0/app/aws/apigw"
	"net/http"
)

func proxyResponseWithS3(ctx context.Context, apiRequest apigw2.ProxyRequest, baseURL string, response *apigw2.ProxyResponse) error {
	preSign, err := storeResponse(ctx, baseURL, apiRequest.JobID, response)
	if err != nil {
		return err
	}
	redirectResponse(response, preSign)
	response.Body = ""
	return nil
}

func redirectResponse(response *apigw2.ProxyResponse, preSign *option.PreSign) {
	if len(response.Headers) == 0 {
		response.Headers = make(map[string]string)
	}
	for k := range preSign.Header {
		response.Headers[k] = preSign.Header.Get(k)
	}
	response.StatusCode = http.StatusTemporaryRedirect
	response.Headers[locationHeader] = preSign.URL
}
