package lambda

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	apigw2 "github.com/viant/datly/v0/app/aws/apigw"
	"os"
)

const functionNameEnvKey = "AWS_LAMBDA_FUNCTION_NAME"

func runInBackground(ctx context.Context, apiRequest apigw2.ProxyRequest) (*apigw2.ProxyResponse, error) {
	function := os.Getenv(functionNameEnvKey)
	apiRequest.AsyncMode = false
	return invokeLambda(ctx, function, apiRequest)
}

func invokeLambda(ctx context.Context, function string, apiRequest apigw2.ProxyRequest) (*apigw2.ProxyResponse, error) {
	apiResponse := &apigw2.ProxyResponse{}
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	srv := lambda.New(sess)
	payload, err := json.Marshal(apiRequest)
	if err != nil {
		return nil, err
	}
	resp, err := srv.InvokeWithContext(ctx, &lambda.InvokeInput{
		FunctionName:   &function,
		Payload:        payload,
		InvocationType: aws.String(lambda.InvocationTypeRequestResponse),
	})
	if len(resp.Payload) > 0 {
		json.Unmarshal(resp.Payload, apiResponse)
	}
	return apiResponse, err
}
