package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/viant/datly/cmd/env"
	"github.com/viant/datly/gateway/runtime/lambda/s3/handler"
	"strconv"
	"time"
)

var (
	Version      = "development"
	BuildTimeInS string
)

func init() {
	if BuildTimeInS != "" {
		seconds, err := strconv.Atoi(BuildTimeInS)
		if err != nil {
			panic(err)
		}

		env.BuildTime = time.Unix(int64(seconds), 0)
	}

	env.BuildType = env.BuildTypeKindLambdaS3
}

func main() {
	lambda.Start(handler.HandleRequest)
}
