package gateway

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	furl "github.com/viant/afs/url"
	"github.com/viant/datly/view"
	"strings"
	"sync"
)

type resourceLoader struct {
	byName view.NamedResources
	sync.Mutex
	WaitGroup sync.WaitGroup
	errors    []error
}

func (n *resourceLoader) load(ctx context.Context, fs afs.Service, URL string) {
	defer n.WaitGroup.Done()
	key := n.key(URL)
	resource, err := view.LoadResourceFromURL(ctx, URL, fs)
	if err != nil {
		n.addErrors(err)
		return
	}
	n.put(key, resource)
}

func (n *resourceLoader) put(name string, resource *view.Resource) {
	n.Mutex.Lock()
	defer n.Mutex.Unlock()
	if len(n.byName) == 0 {
		n.byName = map[string]*view.Resource{}
	}
	n.byName[name] = resource
}

func (n *resourceLoader) addErrors(err error) {
	if err == nil {
		return
	}
	n.Mutex.Lock()
	n.errors = append(n.errors, err)
	n.Mutex.Unlock()
}

func (r *resourceLoader) key(URL string) string {
	_, key := furl.Split(URL, file.Scheme)
	if index := strings.Index(key, "."); index != -1 {
		key = key[:index]
	}
	return key
}
