package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/viant/afs/embed"
	_ "github.com/viant/afsc/aws"
	_ "github.com/viant/afsc/gcp"
	_ "github.com/viant/afsc/gs"
	_ "github.com/viant/afsc/s3"
	_ "github.com/viant/bigquery"
	"github.com/viant/datly/cmd/env"
	"github.com/viant/datly/gateway/runtime/lambda/sqs/handler"
	_ "github.com/viant/scy/kms/blowfish"
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

	env.BuildType = env.BuildTypeKindLambdaSQS
}

func main() {
	lambda.Start(handler.HandleRequest)
}
