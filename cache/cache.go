package cache

import (
	"bytes"
	"context"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"hash/fnv"
	"strconv"
	"time"
)

type Cache struct {
	URL        string
	TimeTiLive time.Duration
	service    afs.Service
}

func NewCache(ttl time.Duration, url string, afsService afs.Service) *Cache {
	return &Cache{
		URL:        url,
		TimeTiLive: ttl,
		service:    afsService,
	}
}

func (c *Cache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	URL := c.Combine(c.URL, key)
	if ok, _ := c.service.Exists(ctx, URL); !ok {
		return nil, false, nil
	}

	data, err := c.service.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, false, err
	}

	expireTime, err := strconv.Atoi(string(data[:19]))
	if err != nil {
		return nil, false, err
	}

	expiryTime := time.Unix(0, int64(expireTime))
	if time.Now().After(expiryTime) {
		return nil, false, c.service.Delete(ctx, URL, option.NewObjectKind(true))
	}

	return data[19:], true, nil
}

func (c *Cache) Delete(ctx context.Context, key string) error {
	return c.service.Delete(ctx, c.URL+key, option.NewObjectKind(true))
}

func (c *Cache) GenerateKey(aKey string) (string, error) {
	hasher := fnv.New64()
	_, err := hasher.Write([]byte(aKey))

	if err != nil {
		return "", err
	}

	entryKey := strconv.Itoa(int(hasher.Sum64()))
	return entryKey, nil
}

func (c *Cache) Upload(ctx context.Context, key string, data []byte) error {
	combinedData := make([]byte, 19+len(data))
	offs := copy(combinedData, strconv.Itoa(int(time.Now().Add(c.TimeTiLive).UnixNano())))
	copy(combinedData[offs:], data)

	return c.service.Upload(ctx, c.Combine(c.URL, key), file.DefaultFileOsMode, bytes.NewBuffer(combinedData))
}

func (c *Cache) Combine(url, key string) string {
	return url + key + ".json"
}
