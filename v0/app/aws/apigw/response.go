package apigw

import "github.com/aws/aws-lambda-go/events"

//ProxyResponse represents a response
type ProxyResponse struct {
	events.APIGatewayProxyResponse
	RawBody    []byte `json:",omitempty"`
	Compressed *bool
}

//IsCompressed returns if compress flag
func (r *ProxyResponse) IsCompressed() bool {
	if r.Compressed == nil {
		return false
	}
	return *r.Compressed
}

func NewProxyResponse() *ProxyResponse {
	return &ProxyResponse{
		APIGatewayProxyResponse: events.APIGatewayProxyResponse{
			Headers:           make(map[string]string),
			MultiValueHeaders: make(map[string][]string),
		},
	}
}
