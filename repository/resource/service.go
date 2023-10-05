package resource

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/storage"
	furl "github.com/viant/afs/url"
	"github.com/viant/cloudless/resource"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/view"
	"strings"
	"sync"
	"time"
)

type (
	Service struct {
		notifier *resource.Tracker
		URL      string
		items    map[string]*version.Resource
		mutex    sync.RWMutex
		fs       afs.Service
		ctx      context.Context
	}
)

func (s *Service) IsCheckDue(t time.Time) bool {
	if s == nil {
		return false
	}
	return s.notifier.IsCheckDue(t)
}
func (s *Service) SyncChanges(ctx context.Context) (bool, error) {
	if s == nil {
		return false, nil
	}
	snap := newSnapshot(s)
	err := s.notifier.Notify(ctx, s.fs, snap.onChange)
	return snap.changed(), err
}

func (s *Service) Lookup(ctx context.Context, key string) (*version.Resource, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	ret, ok := s.items[key]
	if !ok {
		return nil, fmt.Errorf("failed to lookup %s resource", key)
	}
	return ret, nil
}

func (s *Service) Init(ctx context.Context) error {
	snap := newSnapshot(s)
	return s.notifier.Notify(ctx, s.fs, snap.onChange)
}

func (s *Service) key(URL string) string {
	_, key := furl.Split(URL, file.Scheme)
	if index := strings.Index(key, "."); index != -1 {
		key = key[:index]
	}
	return key
}

func (s *Service) onAdd(ctx context.Context, object storage.Object) error {
	aResource, err := view.LoadResourceFromURL(ctx, object.URL(), s.fs)
	if err != nil {
		return err
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	versionResource := &version.Resource{
		Resource: aResource,
	}
	versionResource.Control.SetModTime(object.ModTime())
	versionResource.Control.SetChangeKind(version.ChangeKindModified)
	s.items[s.key(object.URL())] = versionResource
	return nil
}

func (s *Service) onDelete(ctx context.Context, object storage.Object) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	anEntry := s.items[s.key(object.URL())]
	anEntry.Version.Increase()
	anEntry.SetChangeKind(version.ChangeKindDeleted)
	anEntry.Control.SetModTime(object.ModTime())
	return nil
}

func (s *Service) onModify(ctx context.Context, object storage.Object) error {
	aResource, err := view.LoadResourceFromURL(ctx, object.URL(), s.fs)
	if err != nil {
		return err
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	anEntry := s.items[s.key(object.URL())]
	anEntry.Resource = aResource
	anEntry.Version.Increase()
	anEntry.SetChangeKind(version.ChangeKindModified)
	anEntry.Control.SetModTime(object.ModTime())
	return nil
}

func New(ctx context.Context, fs afs.Service, URL string, refreshFrequency time.Duration) (*Service, error) {
	ret := &Service{URL: URL, fs: fs, notifier: resource.New(URL, refreshFrequency), ctx: ctx, items: make(map[string]*version.Resource)}
	err := ret.Init(ctx)
	return ret, err
}
