package bridge

import (
	"github.com/aws/aws-lambda-go/events"
)

//SetCORSHeaderIfNeeded sets CORS headers
func SetCORSHeaderIfNeeded(apiRequest *events.APIGatewayProxyRequest, response *events.APIGatewayProxyResponse) {
	origin, ok := apiRequest.Headers["Origin"]
	if !ok {
		return
	}
	if len(response.Headers) == 0 {
		response.Headers = make(map[string]string)
	}
	response.Headers["Access-Control-Allow-Credentials"] = "true"
	response.Headers["Access-Control-Allow-Origin"] = origin
	response.Headers["Access-Control-Allow-Methods"] = "POST GET"
	response.Headers["Access-Control-Allow-Headers"] = "Content-Type, *"
	response.Headers["Access-Control-Max-Age"] = "120"
}
