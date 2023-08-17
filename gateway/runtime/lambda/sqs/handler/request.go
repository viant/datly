package handler

import (
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/datly/router/async/handler"
	async2 "github.com/viant/xdatly/handler/async"
	"net/http"
)

func HttpRequest(event events.SQSMessage) (*http.Request, *async2.Job, error) {
	recordWithBody := &handler.RecordWithHttp{}
	if len(event.Body) > 0 {
		if err := json.Unmarshal([]byte(event.Body), recordWithBody); err != nil {
			return nil, nil, err
		}
	}

	request, err := recordWithBody.Request()
	if err != nil {
		return nil, nil, err
	}

	return request, recordWithBody.Record, nil
}
