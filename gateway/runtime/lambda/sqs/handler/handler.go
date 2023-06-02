package handler

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	dlambda "github.com/viant/datly/gateway/runtime/lambda"
	"sync"
)

func HandleRequest(ctx context.Context, event *events.SQSEvent) (*events.SQSEventResponse, error) {
	responseBuilder := &EventsResponseBuilder{}
	wg := &sync.WaitGroup{}

	for _, message := range event.Records {
		wg.Add(1)
		go handleRequest(wg, responseBuilder, message)
	}

	wg.Wait()

	for _, respons := range responseBuilder.responses {
		if respons == nil || len(respons.Buffer) == 0 {
			continue
		}

		fmt.Printf("[RESPONSE] %v\n", string(respons.Buffer))
	}

	return &responseBuilder.response, nil
}

func handleRequest(wg *sync.WaitGroup, builder *EventsResponseBuilder, message events.SQSMessage) {
	defer wg.Done()
	response, err := handleWithError(message)
	if err != nil {
		builder.HandleFail(message)
	}

	builder.AddResponse(response)
}

func handleWithError(message events.SQSMessage) (*dlambda.Response, error) {
	request, record, err := HttpRequest(message)
	if err != nil {
		return nil, err
	}

	response := dlambda.NewResponse()
	return response, dlambda.HandleHTTPAsyncRequest(response, request, record)
}
