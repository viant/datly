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
	Resources struct {
		notifier *resource.Tracker
		URL      string
		items    map[string]*version.Resource
		mutex    sync.RWMutex
		fs       afs.Service
		ctx      context.Context
	}
)

func (r *Resources) Lookup(ctx context.Context, key string) (*version.Resource, error) {
	_ = r.notifier.Notify(ctx, r.fs, r.onChange)
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	ret, ok := r.items[key]
	if !ok {
		return nil, fmt.Errorf("failed to lookup %s resource", key)
	}
	return ret, nil
}

func (r *Resources) Init(ctx context.Context) error {
	return r.notifier.Notify(ctx, r.fs, r.onChange)
}

func (r *Resources) onChange(ctx context.Context, object storage.Object, operation resource.Operation) error {
	switch operation {
	case resource.Added:
		return r.onAdd(ctx, object)
	case resource.Deleted:
		return r.onDelete(ctx, object)
	case resource.Modified:
		return r.onModify(ctx, object)
	}
	return nil
}

func (r *Resources) key(URL string) string {
	_, key := furl.Split(URL, file.Scheme)
	if index := strings.Index(key, "."); index != -1 {
		key = key[:index]
	}
	return key
}

func (r *Resources) onAdd(ctx context.Context, object storage.Object) error {
	aResource, err := view.LoadResourceFromURL(ctx, object.URL(), r.fs)
	if err != nil {
		return err
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	versionResource := &version.Resource{
		Resource: aResource,
	}
	versionResource.Control.ModTime = object.ModTime()
	versionResource.Control.ChangeKind = version.ChangeKindModified
	r.items[r.key(object.URL())] = versionResource
	return nil
}

func (r *Resources) onDelete(ctx context.Context, object storage.Object) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	anentry := r.items[r.key(object.URL())]
	anentry.Version.Increase()
	anentry.ChangeKind = version.ChangeKindDeleted
	anentry.Control.ModTime = object.ModTime()
	return nil
}

func (r *Resources) onModify(ctx context.Context, object storage.Object) error {
	aResource, err := view.LoadResourceFromURL(ctx, object.URL(), r.fs)
	if err != nil {
		return err
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	anEntry := r.items[r.key(object.URL())]
	anEntry.Resource = aResource
	anEntry.Version.Increase()
	anEntry.ChangeKind = version.ChangeKindModified
	anEntry.Control.ModTime = object.ModTime()
	return nil
}

func New(ctx context.Context, fs afs.Service, URL string, checkFrequency time.Duration) (*Resources, error) {
	checkFrequency = ensureFrequency(checkFrequency)
	ret := &Resources{URL: URL, fs: fs, notifier: resource.New(URL, checkFrequency), ctx: ctx, items: make(map[string]*version.Resource)}
	err := ret.Init(ctx)
	return ret, err
}

func ensureFrequency(checkFrequency time.Duration) time.Duration {
	if checkFrequency <= time.Millisecond {
		checkFrequency = time.Second
	}
	return checkFrequency
}
