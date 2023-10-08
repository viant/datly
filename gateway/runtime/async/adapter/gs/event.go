package gs

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/storage"
)

// GSEvent represents GS event
type GSEvent struct {
	Bucket                  string `json:"bucket"`
	Name                    string `json:"name"`
	ContentType             string `json:"contentType"`
	CRC32C                  string `json:"crc32c"`
	Etag                    string `json:"etag"`
	Generation              string `json:"generation"`
	ID                      string `json:"id"`
	Kind                    string `json:"kind"`
	Md5Hash                 string `json:"md5Hash"`
	MediaLink               string `json:"mediaLink"`
	Metageneration          string `json:"metageneration"`
	SelfLink                string `json:"selfLink"`
	Size                    string `json:"size"`
	StorageClass            string `json:"storageClass"`
	TimeCreated             string `json:"timeCreated"`
	TimeStorageClassUpdated string `json:"timeStorageClassUpdated"`
	Updated                 string `json:"updated"`
}

// URL returns sourceURL
func (e GSEvent) URL() string {
	return fmt.Sprintf("gs://%s/%s", e.Bucket, e.Name)
}

// StorageObject creates storage object
func (e GSEvent) StorageObject(ctx context.Context, fs afs.Service) (storage.Object, error) {
	return fs.Object(ctx, e.URL())
}

// Exists checks if storage object exists
func (e GSEvent) Exists(ctx context.Context, fs afs.Service) (bool, error) {
	return fs.Exists(ctx, e.URL())
}
