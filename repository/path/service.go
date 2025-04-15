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
		MbusPaths   []*Path
	}
)

func (s *Service) Init(ctx context.Context) (err error) {
	err = s.initPaths(ctx)
	s.initMbusPaths()
	return err
}

func (s *Service) initPaths(ctx context.Context) error {
	pathFile := url.Join(s.URL, PathFileName)
	if exists, _ := s.fs.Exists(ctx, pathFile); !exists {
		if err := s.createPathFiles(ctx); err != nil {
			return err
		}
	} else {
		if err := s.load(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) initMbusPaths() {
	for _, item := range s.Container.Items {
		for i, aPath := range item.Paths {
			if handler := aPath.Handler; handler != nil && handler.MessageBus != "" {
				item.Paths[i].Handler.With = item.With
				s.MbusPaths = append(s.MbusPaths, item.Paths[i])
			}
		}
	}
}

func (s *Service) GetPaths() Container {
	s.mux.RLock()
	ret := s.Container
	s.mux.RUnlock()
	return ret
}

func (s *Service) SyncChanges(ctx context.Context) (bool, error) {
	if s.URL == "" {
		return false, nil
	}
	snap := newSnapshot(s)
	err := s.notifier.Notify(ctx, s.fs, snap.onChange)
	return snap.hasChanged(), err
}

func (s *Service) IsCheckDue(t time.Time) bool {
	if s.URL == "" {
		return false
	}
	if s == nil {
		return false
	}
	return s.notifier.NextCheck().IsZero() || t.After(s.notifier.NextCheck())
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
	key := s.FormatURL(paths.SourceURL)
	s.mux.Lock()
	defer s.mux.Unlock()
	index := len(s.Container.Items)
	s.Container.Items = append(s.Container.Items, paths)
	s.bySourceURL[key] = index
	for _, aPath := range paths.Paths {
		s.byPath[aPath.Key()] = index
		aPath.Version = &paths.Version
	}
}

func (s *Service) lookupRouteBySourceURL(URL string) *Item {
	s.mux.RLock()
	index, ok := s.bySourceURL[URL]
	s.mux.RUnlock()
	if !ok {
		return nil
	}
	s.mux.RLock()
	ret := s.Container.Items[index]
	s.mux.RUnlock()
	return ret
}

func (s *Service) Lookup(aPath *contract.Path) *Path {
	s.mux.RLock()
	defer s.mux.RUnlock()
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
	temp := &Item{}
	if err := yaml.Unmarshal(data, temp); err != nil {
		return nil, err
	}
	sourceURL := candidate.URL()
	if index := strings.Index(sourceURL, rootPath); index != -1 {
		sourceURL = sourceURL[1+index+len(rootPath):]
	}
	anItem := &Item{
		SourceURL: sourceURL,
		Paths:     temp.Paths,
		Settings:  temp.Settings,
		Version:   version.Control{},
		Resource:  temp.Resource,
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
	path := url.Path(object.URL())
	prev := s.lookupRouteBySourceURL(path)
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
	path := url.Path(object.URL())
	prev := s.lookupRouteBySourceURL(path)
	if prev == nil {
		return nil
	}
	prev.Version.SetChangeKind(version.ChangeKindDeleted)
	prev.Version.Increase()

	// TODO delete works fine but after adding back rule file we get panic
	//s.delete(prev, path)
	return nil
}

func (s *Service) delete(item *Item, path string) {
	s.mux.Lock()
	defer s.mux.Unlock()

	delete(s.bySourceURL, path)

	for _, aPath := range item.Paths {
		delete(s.byPath, aPath.Key())
	}

	updatedItems := make([]*Item, len(s.Container.Items)-1)
	j := 0
	for i, _ := range s.Container.Items {
		if s.Container.Items[i] != item {
			updatedItems[j] = s.Container.Items[i]
			j++
		}
	}
	s.Container.Items = updatedItems

}

func New(ctx context.Context, fs afs.Service, URL string, refreshFrequency time.Duration) (*Service, error) {
	ret := &Service{fs: fs, URL: URL, notifier: resource.New(URL, refreshFrequency), bySourceURL: make(map[string]int), byPath: make(map[string]int)}
	if URL == "" {
		return ret, nil
	}
	err := ret.Init(ctx)
	return ret, err
}
