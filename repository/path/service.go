package path

import (
	"bytes"
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/cloudless/resource"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/version"
	"gopkg.in/yaml.v3"
	"path"
	"strings"
	"sync"
	"time"
)

type (
	Service struct {
		Container   Container
		URL         string
		mux         sync.RWMutex
		byPath      map[string]int
		bySourceURL map[string]int
		notifier    *resource.Tracker
		fs          afs.Service
	}
)

func (s *Service) Init(ctx context.Context) (err error) {
	pathFile := url.Join(s.URL, PathFileName)
	if exists, _ := s.fs.Exists(ctx, pathFile); !exists {
		if err = s.createPathFiles(ctx); err != nil {
			return err
		}
	} else {
		if err = s.load(ctx); err != nil {
			return err
		}
	}
	return err
}

func (s *Service) GetPaths() Container {
	s.mux.RLock()
	ret := s.Container
	s.mux.RUnlock()
	return ret
}

func (s *Service) SyncChanges(ctx context.Context) (bool, error) {
	snap := newSnapshot(s)
	err := s.notifier.Notify(ctx, s.fs, snap.onChange)
	return snap.hasChanged(), err
}

func (s *Service) IsCheckDue(t time.Time) bool {
	if s == nil {
		return false
	}
	return s.notifier.IsCheckDue(t)
}

// IncreaseVersion increase version of the all routes
func (s *Service) IncreaseVersion() {
	s.mux.RLock()
	for _, route := range s.Container.Items {
		route.Version.Increase()
	}
	s.mux.RUnlock()
}

func (s *Service) FormatURL(URI string) string {
	if url.IsRelative(URI) {
		return url.Join(s.URL, URI)
	}
	return URI
}

func (s *Service) Append(paths *Item) {
	index := len(s.Container.Items)
	s.Container.Items = append(s.Container.Items, paths)
	s.bySourceURL[s.FormatURL(paths.SourceURL)] = index
	for _, aPath := range paths.Paths {
		s.byPath[aPath.Key()] = index
		aPath.Version = &paths.Version
	}
}

func (s *Service) lookupRouteBySourceURL(URL string) *Item {
	s.mux.RLock()
	defer s.mux.RLock()
	index, ok := s.bySourceURL[URL]
	if !ok {
		return nil
	}
	return s.Container.Items[index]
}

func (s *Service) Lookup(aPath *contract.Path) *Path {
	s.mux.RLock()
	defer s.mux.RLock()
	index, ok := s.byPath[aPath.Key()]
	if !ok {
		return nil
	}
	holder := s.Container.Items[index]
	for _, candidate := range holder.Paths {
		if candidate.Path.Equals(aPath) {
			return candidate
		}
	}
	return nil
}

func (s *Service) createPathFiles(ctx context.Context) error {
	candidates, err := s.fs.List(ctx, s.URL, option.NewRecursive(true))
	if err != nil {
		return err
	}
	rootPath := url.Path(s.URL)
	for _, candidate := range candidates {
		if candidate.IsDir() {
			continue
		}
		if ext := path.Ext(candidate.Name()); ext != ".yaml" && ext != ".yml" {
			continue
		}
		if strings.Contains(candidate.URL(), ".meta/") {
			continue
		}
		paths, err := s.buildPaths(ctx, candidate, rootPath)
		if err != nil {
			return err
		}
		paths.Version.SetModTime(candidate.ModTime())
		s.Append(paths)
	}

	pathFile := s.pathFilename()
	data, err := yaml.Marshal(s.Container)
	if err != nil {
		return err
	}
	err = s.fs.Upload(ctx, pathFile, file.DefaultFileOsMode, bytes.NewReader(data))
	if err != nil {
		return err
	}
	object, err := s.fs.Object(ctx, pathFile)
	if err != nil {
		return err
	}
	s.Container.setModTime(object.ModTime())
	return nil
}

func (s *Service) buildPaths(ctx context.Context, candidate storage.Object, rootPath string) (*Item, error) {
	data, err := s.fs.Download(ctx, candidate)
	if err != nil {
		return nil, err
	}
	aRoutes := &Item{}
	if err := yaml.Unmarshal(data, aRoutes); err != nil {
		return nil, err
	}
	sourceURL := candidate.URL()
	if index := strings.Index(sourceURL, rootPath); index != -1 {
		sourceURL = sourceURL[1+index+len(rootPath):]
	}
	anItem := &Item{
		SourceURL: sourceURL,
		Paths:     aRoutes.Paths,
		Settings:  aRoutes.Settings,
		Version:   version.Control{},
	}
	for _, aPath := range anItem.Paths {
		aPath.inherit(&anItem.Settings)
	}
	return anItem, nil
}

func (s *Service) pathFilename() string {
	return url.Join(s.URL, PathFileName)
}

func (s *Service) load(ctx context.Context) error {
	temp := &Container{}
	pathFile := s.pathFilename()
	object, err := s.fs.Object(ctx, pathFile)
	if err != nil {
		return err
	}
	data, err := s.fs.Download(ctx, object)
	if err != nil {
		return err
	}
	if err = yaml.Unmarshal(data, temp); err != nil {
		return err
	}
	for _, elem := range temp.Items {
		s.Append(elem)
	}
	s.Container.setModTime(object.ModTime())
	return err
}

func (s *Service) onModify(ctx context.Context, object storage.Object) error {
	prev := s.lookupRouteBySourceURL(object.URL())
	if prev != nil && prev.Version.HasChanged(object.ModTime()) {
		return nil
	}
	rootPath := url.Path(s.URL)
	aPath, err := s.buildPaths(ctx, object, rootPath)
	if err != nil {
		return nil
	}
	if prev == nil {
		s.Append(aPath)
		return nil
	}
	prev.Paths = aPath.Paths
	prev.Settings = aPath.Settings
	prev.Version.SetChangeKind(version.ChangeKindModified)
	prev.Version.SetModTime(object.ModTime())
	prev.Version.Increase()
	return nil
}

func (s *Service) onDelete(ctx context.Context, object storage.Object) error {
	prev := s.lookupRouteBySourceURL(object.URL())
	if prev == nil {
		return nil
	}
	prev.Version.SetChangeKind(version.ChangeKindDeleted)
	prev.Version.Increase()
	return nil
}

func New(ctx context.Context, fs afs.Service, URL string, refreshFrequency time.Duration) (*Service, error) {
	ret := &Service{fs: fs, URL: URL, notifier: resource.New(URL, refreshFrequency), bySourceURL: make(map[string]int), byPath: make(map[string]int)}
	err := ret.Init(ctx)
	return ret, err
}
