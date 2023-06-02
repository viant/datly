package handler

import (
	"github.com/aws/aws-lambda-go/events"
	dlambda "github.com/viant/datly/gateway/runtime/lambda"
	"sync"
)

type S3ResponseBuilder struct {
	mux       sync.Mutex
	errors    []error
	succeeded []*events.S3EventRecord
	responses []*dlambda.Response
}

func (e *S3ResponseBuilder) HandleFail(message *events.S3EventRecord, err error) {
	e.mux.Lock()
	defer e.mux.Unlock()
	e.errors = append(e.errors, err)
}

func (e *S3ResponseBuilder) HandleSuccess(record *events.S3EventRecord) {
	e.mux.Lock()
	defer e.mux.Unlock()

	e.succeeded = append(e.succeeded, record)
}

func (e *S3ResponseBuilder) AddResponse(response *dlambda.Response) {
	e.mux.Lock()
	defer e.mux.Unlock()

	e.responses = append(e.responses, response)
}
