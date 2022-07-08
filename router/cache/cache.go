package cache

import (
	"bufio"
	"bytes"
	"context"
	"github.com/goccy/go-json"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"hash/fnv"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type (
	Cache struct {
		TimeToLiveMs int
		Location     string

		_ttl time.Duration
		afs  afs.Service
	}

	LineReadCloser struct {
		reader     *bufio.Reader
		readCloser io.ReadCloser
	}
)

func (c *LineReadCloser) ReadLine() ([]byte, error) {
	line, prefix, err := c.reader.ReadLine()
	if err != nil {
		return nil, err
	}

	var remaining []byte
	for prefix {
		remaining, prefix, err = c.reader.ReadLine()
		if err != nil {
			return nil, err
		}

		line = append(line, remaining...)
	}
	return line, nil
}

func (c *Cache) Init(ctx context.Context) error {
	c._ttl = time.Duration(c.TimeToLiveMs) * time.Millisecond
	c.afs = afs.New()

	return nil
}

func (c *Cache) Get(ctx context.Context, selectors []byte, viewName string) (*Entry, error) {
	key, err := c.Combine(c.Location, selectors, viewName)
	if err != nil {
		return nil, err
	}

	entry := &Entry{
		cache: c,
		meta: Meta{
			View:      viewName,
			Selectors: selectors,
			url:       c.Location + strconv.Itoa(int(key)) + ".json",
		},
		id:  strings.ReplaceAll(uuid.New().String(), "-", ""),
		key: key,
	}

	return entry, c.read(ctx, entry)
}

func (c *Cache) close(ctx context.Context, entry *Entry) error {
	if entry.reader == nil {
		return nil
	}

	actualURL := strings.ReplaceAll(entry.meta.url, ".json"+entry.id, ".json")
	return c.afs.Move(ctx, entry.meta.url, actualURL)
}

func (c *Cache) Put(ctx context.Context, entry *Entry, response []byte, compressionType string) error {
	if entry.reader != nil {
		return entry.Close()
	}

	entry.meta.ExpireAt = Now().Add(c._ttl)
	entry.meta.CompressionType = compressionType
	entry.meta.Size = len(response)

	metaBytes, err := json.Marshal(entry.meta)
	if err != nil {
		return err
	}

	writeCloser, err := c.afs.NewWriter(ctx, entry.meta.url, file.DefaultFileOsMode)
	if err != nil {
		return err
	}

	defer writeCloser.Close()
	if err = c.write(writeCloser, metaBytes, []byte("\n"), response); err != nil {
		_ = c.afs.Delete(ctx, entry.meta.url)
		return err
	}

	return c.close(ctx, entry)
}

func (c *Cache) Combine(location string, selectors []byte, name string) (uint64, error) {
	keySize := len(location) + len(selectors) + len(name) + 2
	key := make([]byte, keySize)
	offset := 0

	offset += copy(key[offset:], location)
	offset += copy(key[offset:], "/")
	offset += copy(key[offset:], selectors)
	offset += copy(key[offset:], "/")
	offset += copy(key[offset:], name)

	hasher := fnv.New64a()
	_, err := hasher.Write(key)

	return hasher.Sum64(), err
}

func (c *Cache) read(ctx context.Context, entry *Entry) error {
	if ok, err := c.afs.Exists(ctx, entry.meta.url); !ok || err != nil {
		return nil
	}

	readCloser, err := c.afs.OpenURL(ctx, entry.meta.url)
	if isRateError(err) || isPreConditionError(err) {
		return nil
	}

	if err != nil {
		return err
	}

	reader := &LineReadCloser{
		reader:     bufio.NewReader(readCloser),
		readCloser: readCloser,
	}

	line, err := reader.ReadLine()
	if err != nil {
		return err
	}

	ok, err := c.validateMeta(ctx, entry, line)
	if err != nil || !ok {
		return err
	}

	entry.reader = reader
	return nil
}

func (c *Cache) validateMeta(ctx context.Context, entry *Entry, line []byte) (bool, error) {
	cachedMeta := &Meta{}
	if err := json.Unmarshal(line, cachedMeta); err != nil {
		return false, err
	}

	now := Now()
	if now.After(cachedMeta.ExpireAt) || !bytes.Equal(entry.meta.Selectors, cachedMeta.Selectors) || entry.meta.View != cachedMeta.View {
		return false, c.afs.Delete(ctx, entry.meta.url)
	}

	return true, nil
}

func (c *Cache) write(writer io.WriteCloser, data ...[]byte) error {
	var err error
	for _, value := range data {
		if _, err = writer.Write(value); err != nil {
			return err
		}
	}

	return nil
}

func isRateError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), strconv.Itoa(http.StatusTooManyRequests))
}

func isPreConditionError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), strconv.Itoa(http.StatusPreconditionFailed))
}
