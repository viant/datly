package s3

import (
	"context"
	"fmt"
	"github.com/aws/aws-lambda-go/events"
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
)

// S3Event represents S3 Events
type S3Event events.S3Event

func (e S3Event) URL() string {
	return fmt.Sprintf("s3://%s/%s", e.Records[0].S3.Bucket.Name, e.Records[0].S3.Object.Key)
}

// StorageObject creates storage object
func (e S3Event) StorageObject(ctx context.Context, fs afs.Service) (storage.Object, error) {
	return fs.Object(ctx, e.URL())
}

// Exists checks if storage object exists
func (e S3Event) Exists(ctx context.Context, fs afs.Service) (bool, error) {
	return fs.Exists(ctx, e.URL())
}
