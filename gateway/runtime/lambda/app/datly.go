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
	dlambda "github.com/viant/datly/gateway/runtime/lambda"
	_ "github.com/viant/scy/kms/blowfish"
)

func main() {
	lambda.Start(dlambda.HandleRequest)
}
