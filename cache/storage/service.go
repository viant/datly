package storage

import (
	"bytes"
	"context"
	"datly/cache"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"io/ioutil"
	"time"
)

//service trivial storage based cache implementation
type service struct {
	fs      afs.Service
	baseURL string
}

//Put puts value to cache
func (s *service) Put(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	URL := url.Join(s.baseURL, key)
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(data))
}

//Get returns value from a cache
func (s *service) Get(ctx context.Context, key string) ([]byte, *time.Time, error) {
	URL := url.Join(s.baseURL, key)
	object, _ := s.fs.Object(ctx, URL, option.NewObjectKind(true))
	if object == nil {
		return nil, nil, nil
	}
	reader, err := s.fs.DownloadWithURL(ctx, URL, option.NewObjectKind(true))
	if err != nil {
		return nil, nil, err
	}
	_ = reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to load cache %v", URL)
	}
	modified := object.ModTime()
	return data, &modified, nil
}

//Delete deletes cache value
func (s *service) Delete(ctx context.Context, key string) error {
	URL := url.Join(s.baseURL, key)
	return s.fs.Delete(ctx, URL, option.NewObjectKind(true))
}

//New create a storage base cache service
func New(baseURL string, fs afs.Service) cache.Service {
	return &service{
		fs:      fs,
		baseURL: baseURL,
	}
}
