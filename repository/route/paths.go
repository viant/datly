package route

import (
	"bytes"
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/storage"
	"github.com/viant/afs/url"
	"github.com/viant/datly/gateway/router/marshal"
	"github.com/viant/datly/repository/component"
	"github.com/viant/datly/repository/version"
	"gopkg.in/yaml.v3"
	"path"
	"strings"
	"time"
)

const (
	PathFileName = "paths.yaml"
)

type (
	Paths struct {
		URL         string     `yaml:"-" json:"-"`
		ModTime     *time.Time `yaml:"ModTime,omitempty" json:",omitempty"`
		Routes      []*Route   `yaml:"Routes"`
		byPath      map[string]int
		bySourceURL map[string]int
		fs          afs.Service
	}

	Settings struct {
		APIKey      *APIKey      `json:",omitempty"  yaml:"APIKey,omitempty"`
		Cors        *Cors        `json:",omitempty"  yaml:"Cors,omitempty"`
		Compression *Compression `json:",omitempty"  yaml:"Compression,omitempty"`
		Redirect    *Redirect    `json:",omitempty"  yaml:"Redirect,omitempty"`
		Logger      *Logger      `json:",omitempty"  yaml:"Logger,omitempty"`
	}

	Element struct {
		component.Path `yaml:",inline"`
		Settings       `yaml:",inline"`
		Transforms     marshal.Transforms `json:"Transforms,omitempty" yaml:"Transforms,omitempty" `
		SourceURL      string             `yaml:"-" json:"-"`
		Version        *version.Control   `yaml:"-" json:"-"`
	}

	Route struct {
		SourceURL string     `yaml:"SourceURL"`
		Routes    []*Element `yaml:"Routes"`
		Settings  `yaml:",inline"`
		Version   version.Control `yaml:"Version,omitempty"`
	}
)

func (p *Paths) pathFilename() string {
	return url.Join(p.URL, PathFileName)
}

func (p *Paths) Init(ctx context.Context) (err error) {
	pathFile := url.Join(p.URL, PathFileName)
	if exists, _ := p.fs.Exists(ctx, pathFile); !exists {
		if err = p.createPathFiles(ctx); err != nil {
			return err
		}
	} else {
		if err = p.load(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (p *Paths) setModTime(ts time.Time) {
	p.ModTime = &ts
}
func (p *Paths) Append(paths *Route) {
	index := len(p.Routes)
	p.Routes = append(p.Routes, paths)
	if len(p.byPath) == 0 {
		p.byPath = map[string]int{}
		p.bySourceURL = map[string]int{}
	}
	p.byPath[paths.SourceURL] = index
	for _, aPath := range paths.Routes {
		p.byPath[aPath.Key()] = index
	}
	for _, aPath := range paths.Routes {
		if url.IsRelative(paths.SourceURL) {
			aPath.SourceURL = url.Join(p.URL, paths.SourceURL)
		}
		aPath.Version = &paths.Version
	}
}

func (p *Paths) Lookup(aPath *component.Path) *Element {
	index, ok := p.byPath[aPath.Key()]
	if !ok {
		return nil
	}
	holder := p.Routes[index]
	for _, candidate := range holder.Routes {
		if candidate.Path.Equals(aPath) {
			return candidate
		}
	}
	return nil
}

func (p *Paths) createPathFiles(ctx context.Context) error {
	candidates, err := p.fs.List(ctx, p.URL, option.NewRecursive(true))
	if err != nil {
		return err
	}
	rootPath := url.Path(p.URL)
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
		paths, err := p.buildPaths(ctx, candidate, rootPath)
		if err != nil {
			return err
		}
		paths.Version.ModTime = candidate.ModTime()
		p.Append(paths)
	}

	pathFile := p.pathFilename()
	data, err := yaml.Marshal(p)
	if err != nil {
		return err
	}
	err = p.fs.Upload(ctx, pathFile, file.DefaultFileOsMode, bytes.NewReader(data))
	if err != nil {
		return err
	}
	object, err := p.fs.Object(ctx, pathFile)
	if err != nil {
		return err
	}
	p.setModTime(object.ModTime())
	return nil
}

func (p *Paths) buildPaths(ctx context.Context, candidate storage.Object, rootPath string) (*Route, error) {
	data, err := p.fs.Download(ctx, candidate)
	if err != nil {
		return nil, err
	}
	aRoutes := &Route{}
	if err := yaml.Unmarshal(data, aRoutes); err != nil {
		return nil, err
	}
	sourceURL := candidate.URL()
	if index := strings.Index(sourceURL, rootPath); index != -1 {
		sourceURL = sourceURL[1+index+len(rootPath):]
	}
	paths := &Route{
		SourceURL: sourceURL,
		Routes:    aRoutes.Routes,
		Settings:  aRoutes.Settings,
		Version:   version.Control{},
	}
	return paths, nil
}

func (p *Paths) load(ctx context.Context) error {
	temp := &Paths{}
	pathFile := p.pathFilename()
	object, err := p.fs.Object(ctx, pathFile)
	if err != nil {
		return err
	}
	data, err := p.fs.Download(ctx, object)
	if err != nil {
		return err
	}
	if err = yaml.Unmarshal(data, temp); err != nil {
		return err
	}
	for _, elem := range temp.Routes {
		p.Append(elem)
	}
	modTime := object.ModTime()
	p.ModTime = &modTime
	return err
}

func New(ctx context.Context, fs afs.Service, URL string) (*Paths, error) {
	ret := &Paths{fs: fs, URL: URL}
	err := ret.Init(ctx)
	return ret, err
}
