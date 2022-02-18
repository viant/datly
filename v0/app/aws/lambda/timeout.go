package lambda

import (
	"context"
	"encoding/json"
	apigw2 "github.com/viant/datly/v0/app/aws/apigw"
	"github.com/viant/datly/v0/app/aws/bridge"
	"github.com/viant/datly/v0/base/contract"
)

func handleAPIGWTimeout(baseURL string, apiRequest apigw2.ProxyRequest) *apigw2.ProxyResponse {
	status := contract.NewStatusInfo()
	status.StartTime = apiRequest.Created
	status.JobID = apiRequest.TraceID
	status.OnDone()
	data, _ := json.Marshal(status)
	response := &apigw2.ProxyResponse{}
	response.Body = string(data)
	preSign, err := storeResponse(context.Background(), baseURL, apiRequest.TraceID, response)
	if err != nil {
		return bridge.ErrorAPIGatewayResponse(err)
	}
	redirectResponse(response, preSign)
	return response
}
