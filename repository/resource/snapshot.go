package resource

import (
	"context"
	"github.com/viant/afs/storage"
	"github.com/viant/cloudless/resource"
	"path"
	"sync/atomic"
)

type snapshot struct {
	service  *Service
	added    int32
	modified int32
	deleted  int32
}

func (s *snapshot) changed() bool {
	return s.added > 0 || s.modified > 0 || s.deleted > 0
}

func (s *snapshot) onChange(ctx context.Context, object storage.Object, operation resource.Operation) error {
	ext := path.Ext(object.Name())
	switch ext {
	case ".yaml", ".yml":
		//track only yaml rule files
	default:
		return nil
	}
	switch operation {
	case resource.Added:
		atomic.AddInt32(&s.added, 1)
		return s.service.onAdd(ctx, object)
	case resource.Deleted:
		atomic.AddInt32(&s.deleted, 1)
		return s.service.onDelete(ctx, object)
	case resource.Modified:
		atomic.AddInt32(&s.modified, 1)
		return s.service.onModify(ctx, object)
	}
	return nil
}

func newSnapshot(service *Service) *snapshot {
	return &snapshot{service: service}
}
