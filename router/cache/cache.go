package cache

import (
	"bytes"
	"context"
	goJson "encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"hash/fnv"
	"io/ioutil"
	"strconv"
	"sync"
	"time"
)

type (
	Cache struct {
		TimeToLiveMs int
		StorageURL   string

		_ttl time.Duration
		*cache
	}

	cache struct {
		mutex *sync.Mutex
		afs.Service
	}
)

func (c *Cache) Init(ctx context.Context) error {
	c.cache = &cache{
		mutex:   &sync.Mutex{},
		Service: afs.New(),
	}

	c._ttl = time.Duration(c.TimeToLiveMs) * time.Millisecond
	return nil
}

func (c *Cache) Get(ctx context.Context, entry *Entry) error {
	aKey := append([]byte(entry.View.Name), entry.Selectors...)
	entryKey, err := c.hashKey(aKey)
	if err != nil {
		return err
	}

	cacheUri := url.Join(c.StorageURL, entryKey)
	exists, err := c.Exists(ctx, cacheUri, option.NewObjectKind(true))
	entry.found = false
	entry.key = entryKey

	if !exists {
		return nil
	}

	reader, err := c.OpenURL(ctx, cacheUri, option.NewObjectKind(true))
	if err != nil {
		return err
	}

	defer reader.Close()
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}

	if len(data) < 19 {
		return fmt.Errorf("invalid cache format")
	}

	expireTime, err := strconv.Atoi(string(data[:19]))
	if err != nil {
		return err
	}

	expiryTime := time.Unix(0, int64(expireTime))
	if time.Now().After(expiryTime) {
		return c.Delete(ctx, cacheUri, option.NewObjectKind(true))
	}

	value := new(Value)
	if err = goJson.Unmarshal(data[19:], value); err != nil {
		return err
	}

	if !bytes.Equal(value.Selectors, entry.Selectors) || value.ViewId != entry.View.ID() {
		return c.Delete(ctx, cacheUri, option.NewObjectKind(true))
	}

	entry.found = true
	entry.result = value.Data
	return nil
}

func (c *Cache) hashKey(aKey []byte) (string, error) {
	hasher := fnv.New64()
	_, err := hasher.Write(aKey)

	if err != nil {
		return "", err
	}

	entryKey := strconv.Itoa(int(hasher.Sum64()))
	return entryKey, nil
}

func (c *Cache) Put(ctx context.Context, entry *Entry) error {
	URL := url.Join(c.StorageURL, entry.key)
	expiryAt := time.Now().Add(c._ttl)
	expiry := fmt.Sprintf("%19d", expiryAt.UnixNano())
	buf := new(bytes.Buffer)
	buf.WriteString(expiry)

	dataBytes, err := goJson.Marshal(entry.Data)
	if err != nil {
		return err
	}

	cacheValue := Value{
		Selectors: entry.Selectors,
		Data:      dataBytes,
		ViewId:    entry.View.ID(),
	}

	valueBytes, err := goJson.Marshal(cacheValue)
	if err != nil {
		return err
	}

	buf.Write(valueBytes)
	return c.Upload(ctx, URL, file.DefaultFileOsMode, buf)
}
