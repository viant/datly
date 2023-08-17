package sqs

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/viant/datly/router/async/handler"
	"net/http"
)

type Handler struct {
	queueURL *string
	sqs      *sqs.SQS
}

func NewHandler(ctx context.Context, queueName string) (*Handler, error) {
	if queueName == "" {
		queueName = "DATLY_JOBS"
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	srv := sqs.New(sess)
	url, err := ensureQueryURL(ctx, srv, queueName)
	if err != nil {
		return nil, err
	}

	return &Handler{
		queueURL: url,
		sqs:      srv,
	}, nil
}

func ensureQueryURL(ctx context.Context, srv *sqs.SQS, name string) (*string, error) {
	// check if queue exists
	queue, err := srv.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &name,
	})

	if err == nil {
		return queue.QueueUrl, nil
	}

	// if not try to create new  one
	createdQueue, err := srv.CreateQueue(&sqs.CreateQueueInput{
		QueueName: &name,
	})

	if err == nil {
		return createdQueue.QueueUrl, nil
	}

	// check if queue was created between first check and create queue attempt
	queue, err = srv.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: &name,
	})

	if err == nil {
		return queue.QueueUrl, nil
	}

	return nil, err
}

func (h *Handler) Handle(ctx context.Context, record *handler.RecordWithHttp, request *http.Request) error {
	marshal, err := json.Marshal(record)
	if err != nil {
		return err
	}

	headers := request.Header

	attributes := map[string]*sqs.MessageAttributeValue{}
	for key, strings := range headers {
		values := make([]*string, 0, len(strings))
		for i := range strings {
			values = append(values, &strings[i])
		}

		attributes[key] = &sqs.MessageAttributeValue{
			StringListValues: values,
		}
	}

	messageBody := string(marshal)
	_, err = h.sqs.SendMessage(&sqs.SendMessageInput{
		QueueUrl:          h.queueURL,
		MessageBody:       &messageBody,
		MessageAttributes: attributes,
	})

	return err
}
