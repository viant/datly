package handler

import (
	"github.com/aws/aws-lambda-go/events"
	dlambda "github.com/viant/datly/gateway/runtime/lambda"
	"sync"
)

type EventsResponseBuilder struct {
	mux       sync.Mutex
	response  events.SQSEventResponse
	responses []*dlambda.Response
}

func (e *EventsResponseBuilder) HandleFail(message events.SQSMessage) {
	e.mux.Lock()
	defer e.mux.Unlock()
	e.response.BatchItemFailures = append(e.response.BatchItemFailures, events.SQSBatchItemFailure{
		ItemIdentifier: message.MessageId,
	})
}

func (e *EventsResponseBuilder) AddResponse(response *dlambda.Response) {
	e.mux.Lock()
	defer e.mux.Unlock()
	e.responses = append(e.responses, response)
}
