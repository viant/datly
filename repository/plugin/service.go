package plugin

import (
	"context"
	"github.com/viant/afs"
	"github.com/viant/cloudless/resource"
	"github.com/viant/datly/cmd/env"
	"github.com/viant/datly/view/extension"
	pbuild "github.com/viant/pgo/build"
	"github.com/viant/pgo/manager"
	"time"
)

type (
	Service struct {
		fs       afs.Service
		notifier *resource.Tracker
		URL      string
		plugins  *manager.Service
	}
)

func (s *Service) SyncChanges(ctx context.Context) (bool, error) {
	if s == nil {
		return false, nil
	}
	if exists, _ := s.fs.Exists(ctx, s.URL); !exists {
		return false, nil
	}
	snap := newSnapshot(s)
	if err := s.notifier.Notify(ctx, s.fs, snap.onChange); err != nil {
		return false, err
	}
	hasChanges := snap.changes > 0
	if hasChanges {
		extension.Config.MergeFrom(snap.registry)
	}
	return snap.changed(), nil
}

func (s *Service) IsCheckDue(t time.Time) bool {
	if s == nil {
		return false
	}
	return s.notifier.IsCheckDue(t)
}

func (s *Service) init(ctx context.Context) error {
	_, err := s.SyncChanges(ctx)
	return err
}

func New(ctx context.Context, fs afs.Service, URL string, syncFrequency time.Duration) (*Service, error) {
	ret := &Service{
		URL:      URL,
		fs:       fs,
		plugins:  manager.New(pbuild.NewSequenceChangeNumber(env.BuildTime)),
		notifier: resource.New(URL, syncFrequency),
	}
	err := ret.init(ctx)
	return ret, err
}
