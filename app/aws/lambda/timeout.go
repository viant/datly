package lambda

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/app/aws/apigw"
	"github.com/viant/datly/app/aws/bridge"
	"github.com/viant/datly/base/contract"
)

func handleAPIGWTimeout(baseURL string, apiRequest apigw.ProxyRequest) *apigw.ProxyResponse {
	status := contract.NewStatusInfo()
	status.StartTime = apiRequest.Created
	status.JobID = apiRequest.TraceID
	status.OnDone()
	data, _ := json.Marshal(status)
	response := &apigw.ProxyResponse{}
	response.Body = string(data)
	preSign, err := storeResponse(context.Background(), baseURL, apiRequest.TraceID, response)
	if err != nil {
		return bridge.ErrorAPIGatewayResponse(err)
	}
	redirectResponse(response, preSign)
	return response
}
