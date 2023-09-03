package handler

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/viant/datly/gateway/router/async/handler"
	async2 "github.com/viant/xdatly/handler/async"
	"io"
	"net/http"
)

type nopCloser struct {
	io.Reader
}

func (np *nopCloser) Read(p []byte) (n int, err error) {
	return np.Reader.Read(p)
}

func (np *nopCloser) Close() error {
	return nil
}

func HttpRequest(ctx context.Context, s3Client *s3.Client, event *events.S3EventRecord) (*http.Request, *async2.Job, error) {
	object, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &event.S3.Bucket.Name,
		Key:    &event.S3.Object.URLDecodedKey,
	})

	if err != nil {
		return nil, nil, err
	}

	content, err := io.ReadAll(object.Body)
	defer object.Body.Close()
	if err != nil {
		return nil, nil, err
	}

	aRecord := &handler.RecordWithHttp{}
	if err = json.Unmarshal(content, aRecord); err != nil {
		return nil, nil, err
	}

	request, err := aRecord.Request()
	if err != nil {
		return nil, nil, err
	}

	return request, aRecord.Record, nil
}
