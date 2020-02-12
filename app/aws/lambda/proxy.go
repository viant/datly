package lambda

import (
	"context"
	"github.com/viant/afs/option"
	"github.com/viant/datly/app/aws/apigw"
	"net/http"
)

func proxyResponseWithS3(ctx context.Context, apiRequest apigw.ProxyRequest, baseURL string, response *apigw.ProxyResponse) error {
	preSign, err := storeResponse(ctx, baseURL, apiRequest.JobID, response)
	if err != nil {
		return err
	}
	redirectResponse(response, preSign)
	response.Body = ""
	return nil
}

func redirectResponse(response *apigw.ProxyResponse, preSign *option.PreSign) {
	if len(response.Headers) == 0 {
		response.Headers = make(map[string]string)
	}
	for k := range preSign.Header {
		response.Headers[k] = preSign.Header.Get(k)
	}
	response.StatusCode = http.StatusTemporaryRedirect
	response.Headers[locationHeader] = preSign.URL
}
