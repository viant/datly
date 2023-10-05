package path

import (
	"context"
	"github.com/viant/afs/storage"
	"github.com/viant/cloudless/resource"
	"path"
	"sync/atomic"
)

type snapshot struct {
	service  *Service
	modified int32
	deleted  int32
}

func (r *snapshot) hasChanged() bool {
	return r.modified > 0 || r.deleted > 0
}

func (r *snapshot) onChange(ctx context.Context, object storage.Object, operation resource.Operation) error {
	if object.IsDir() {
		return nil
	}
	ext := path.Ext(object.Name())
	switch operation {
	case resource.Added, resource.Modified:
		switch ext {
		case ".yaml", ".yml":
			atomic.AddInt32(&r.modified, 1)
			return r.service.onModify(ctx, object)
		case ".sql":
			//TODO extract parent folder and increment version for coresponding rule
		}
	case resource.Deleted:
		switch ext {
		case ".yaml", ".yml":
			atomic.AddInt32(&r.deleted, 1)
			return r.service.onDelete(ctx, object)
		case ".sql":
			//TODO extract parent folder and increment version for coresponding rule
		}
	}
	return nil
}

func newSnapshot(service *Service) *snapshot {
	return &snapshot{service: service}
}
