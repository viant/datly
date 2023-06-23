package handler

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	dlambda "github.com/viant/datly/gateway/runtime/lambda"
	"github.com/viant/datly/shared"
	"github.com/viant/toolbox"
	"sync"
)

func HandleRequest(ctx context.Context, event *events.S3Event) error {
	fmt.Printf("[INFO] event incoming\n")
	toolbox.Dump(event)
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}

	s3Client := s3.NewFromConfig(cfg)

	responseBuilder := &S3ResponseBuilder{}
	wg := &sync.WaitGroup{}

	for i := range event.Records {
		wg.Add(1)
		go handleRequest(wg, responseBuilder, s3Client, &event.Records[i])
	}

	wg.Wait()

	for _, respons := range responseBuilder.responses {
		if respons == nil || len(respons.Buffer) == 0 {
			continue
		}

		fmt.Printf("[RESPONSE] %v\n", string(respons.Buffer))
	}

	//for _, record := range responseBuilder.succeeded {
	//	if _, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
	//		Bucket: &record.S3.Bucket.Name,
	//		Column:    &record.S3.Object.URLDecodedKey,
	//	}); err != nil {
	//		fmt.Printf("[ERROR] error occurred when tried to delete S3 file %v under bucket %v\n", record.S3.Object.URLDecodedKey, record.S3.Bucket.Name)
	//	}
	//}

	if len(responseBuilder.errors) > 0 {
		return shared.CombineErrors("[ERROR] ", responseBuilder.errors)
	}

	return nil
}

func handleRequest(wg *sync.WaitGroup, builder *S3ResponseBuilder, client *s3.Client, record *events.S3EventRecord) {
	defer wg.Done()
	response, err := handleWithError(context.Background(), record, client)
	if err != nil {
		builder.HandleFail(record, err)
	} else {
		builder.HandleSuccess(record)
	}

	builder.AddResponse(response)
}

func handleWithError(ctx context.Context, message *events.S3EventRecord, client *s3.Client) (*dlambda.Response, error) {
	request, record, err := HttpRequest(ctx, client, message)
	fmt.Printf("Building request %v, %+v, %v\n", err, request, record)
	if err != nil {
		return nil, err
	}

	response := dlambda.NewResponse()

	return response, dlambda.HandleHTTPAsyncRequest(response, request, record)
}
