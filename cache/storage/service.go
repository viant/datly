package storage

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/datly/cache"
	"github.com/viant/toolbox"
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
	expiryAt := time.Now().Add(ttl)
	expiry := fmt.Sprintf("%19d", expiryAt.UnixNano())
	buf := new(bytes.Buffer)
	buf.WriteString(expiry)
	buf.Write(data)
	return s.fs.Upload(ctx, URL, file.DefaultFileOsMode, buf)
}

//Get returns value from a cache
func (s *service) Get(ctx context.Context, key string) ([]byte, error) {

	URL := url.Join(s.baseURL, key)
	exists, err := s.fs.Exists(ctx, URL, option.NewObjectKind(true))
	if ! exists {
		return nil, err
	}
	reader, err := s.fs.DownloadWithURL(ctx, URL, option.NewObjectKind(true))
	if err != nil {
		return nil, err
	}
	_ = reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load cache %v", URL)
	}
	unixExpiry := toolbox.AsInt(string(data[0:19]))
	expiryTime := time.Unix(0, int64(unixExpiry))
	if time.Now().After(expiryTime) {
		err := s.Delete(ctx, key)
		return nil, err
	}
	return data[19:], nil
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
