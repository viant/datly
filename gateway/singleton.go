package gateway

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"os"
	"sync"
)

var service *Service
var err error
var once sync.Once

func Singleton(ctx context.Context, options ...Option) (*Service, error) {
	once.Do(func() {
		service, err = New(ctx, options...)
	})
	if err != nil {
		once = sync.Once{}
	}
	return service, err
}

func NewFs(configURL string, fs afs.Service) afs.Service {
	if os.Getenv("DATLY_FS") == "cfs" {
		ParentURL, _ := url.Split(configURL, file.Scheme)
		return NewCacheFs(ParentURL)
	}
	return fs
}

func ResetSingleton() {
	once = sync.Once{}
	if service != nil {
		_ = service.Close()
	}
}
