package gateway

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/cloudless/resource"
)

type Tracker struct {
	assets   map[string]bool
	url      string
	fs       afs.Service
	notifier *resource.Tracker
}

func NewNotifier(URL string, fs afs.Service, notifier *resource.Tracker) *Tracker {
	return &Tracker{
		assets:   map[string]bool{},
		url:      URL,
		fs:       fs,
		notifier: notifier,
	}
}

func (t *Tracker) Notify(ctx context.Context, fs afs.Service, callback func(URL string, operation resource.Operation)) error {
	exists, err := fs.Exists(ctx, t.url)
	if !exists || err != nil {
		for key := range t.assets {
			callback(key, resource.Deleted)
		}

		t.assets = map[string]bool{}
		return nil
	}

	err = t.notifier.Notify(ctx, fs, func(URL string, operation resource.Operation) {
		switch operation {
		case resource.Deleted:
			delete(t.assets, URL)
		case resource.Added:
			t.assets[URL] = true
		}
		callback(URL, operation)
	})

	return err
}
