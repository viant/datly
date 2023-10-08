package lambda

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/gateway/runtime/async/adapter/s3"
	"github.com/viant/datly/gateway/runtime/serverless"
	"github.com/viant/xdatly/handler/response"
)

func HandleRequest(ctx context.Context, event s3.S3Event) (*response.Status, error) {
	status := &response.Status{Status: "ok"}
	err := handleEvent(ctx, event, status)
	if err != nil {
		status.Status = "error"
		status.Message = err.Error()
	}
	return status, nil
}

func handleEvent(ctx context.Context, event s3.S3Event, status *response.Status) error {
	if exists, _ := event.Exists(ctx, afs.New()); exists {
		status.Status = "not_found"
		return nil
	}

	service, err := serverless.GetService()
	if err != nil {
		return err
	}
	object, err := event.StorageObject(ctx, afs.New())
	if err != nil {
		return err
	}
	router, _ := service.Router()
	if router == nil {
		err = fmt.Errorf("router was nil")
	} else {
		err = router.DispatchStorageEvent(ctx, object)
	}
	return err
}
