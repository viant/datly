package env

const (
	BuildTypeKindLambda    BuildTypeKind = "aws-lambda"
	BuildTypeKindAPIGW     BuildTypeKind = "apigw"
	BuildTypeKindLambdaSQS BuildTypeKind = "aws-sqs"
	BuildTypeKindLambdaS3  BuildTypeKind = "aws-s3"
)

type BuildTypeKind string
