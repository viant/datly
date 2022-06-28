package cache

import (
	"bytes"
	"context"
	goJson "encoding/json"
	"github.com/viant/afs"
	"github.com/viant/datly/cache"
	"time"
)

type (
	Cache struct {
		TimeToLiveMs int
		StorageURL   string

		_ttl    time.Duration
		service *cache.Cache
	}
)

func (c *Cache) Init(ctx context.Context) error {
	c._ttl = time.Duration(c.TimeToLiveMs) * time.Millisecond
	c.service = cache.NewCache(c._ttl, c.StorageURL, afs.New())

	return nil
}

func (c *Cache) Get(ctx context.Context, entry *Entry) error {
	var err error
	entry.key, err = c.service.GenerateKey(entry.View.Name + string(entry.Selectors))
	if err != nil {
		return err
	}

	dataBytes, found, err := c.service.Get(ctx, entry.key)
	if err != nil || !found {
		return err
	}

	value := new(Value)
	if err = goJson.Unmarshal(dataBytes, value); err != nil {
		return err
	}

	if !bytes.Equal(value.Selectors, entry.Selectors) || entry.View.Name != value.ViewName {
		return c.service.Delete(ctx, entry.key)
	}

	entry.found = true
	entry.result = value.Data
	return nil
}

func (c *Cache) Put(ctx context.Context, entry *Entry) error {
	dataBytes, err := goJson.Marshal(entry.Data)
	if err != nil {
		return err
	}

	cacheValue := Value{
		Selectors: entry.Selectors,
		Data:      dataBytes,
		ViewName:  entry.View.Name,
	}

	valueBytes, err := goJson.Marshal(cacheValue)
	if err != nil {
		return err
	}

	return c.service.Upload(ctx, entry.key, valueBytes)
}
