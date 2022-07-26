package view

import (
	"context"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/aerospike"
	"github.com/viant/sqlx/io/read/cache/afs"
	"strconv"
	"strings"
	"time"
)

type (
	Cache struct {
		Type         string
		Location     string
		SetName      string
		TimeToLiveMs int
		PartSize     int
		cache        cache.Cache

		initialized bool
	}

	CacheType string
)

const (
	defaultType   = ""
	afsType       = "afs"
	aerospikeType = "aerospike"
)

func (c *Cache) init(ctx context.Context, viewName string) error {
	if c.initialized {
		return nil
	}
	c.initialized = true

	if c.Location == "" {
		return fmt.Errorf("view %v cache Location can't be empty", viewName)
	}

	if c.TimeToLiveMs == 0 {
		return fmt.Errorf("view %v cache TimeToLiveMs can't be empty", viewName)
	}

	aCache, err := c.cacheService(viewName)
	if err != nil {
		return fmt.Errorf("view %v error: %w", viewName, err)
	}

	c.cache = aCache
	return nil
}

func (c *Cache) cacheService(viewName string) (cache.Cache, error) {
	switch c.Type {
	case aerospikeType:
		return c.aerospikeCache()
	default:
		return afs.NewCache(c.Location, time.Duration(c.TimeToLiveMs)*time.Millisecond, viewName, option.NewStream(c.PartSize, 0))
	}
}

func (c *Cache) aerospikeCache() (cache.Cache, error) {
	if c.SetName == "" {
		return nil, fmt.Errorf("aerospike cache SetName cannot be empty")
	}

	host, port, namespace, err := c.split(c.Location)
	if err != nil {
		return nil, err
	}

	client, err := as.NewClient(host, port)
	if err != nil {
		return nil, err
	}

	return aerospike.New(namespace, c.SetName, client, uint32(time.Duration(c.TimeToLiveMs)*time.Second/time.Millisecond))
}

func (c *Cache) Service() cache.Cache {
	return c.cache
}

func (c *Cache) split(location string) (host string, port int, namespace string, err error) {
	actualScheme := url.Scheme(location, "")

	hostPart, namespace := url.Split(location, actualScheme)

	if namespace == "" {
		return "", 0, "", c.unsupportedLocationFormat(location)
	}

	hostStart := 0
	if actualScheme != "" {
		hostStart = len(actualScheme) + 3
	}

	segments := strings.Split(hostPart[hostStart:len(hostPart)-1], ":")
	if len(segments) != 2 {
		return "", 0, "", c.unsupportedLocationFormat(location)
	}

	port, err = strconv.Atoi(segments[1])
	if err != nil {
		return "", 0, "", err
	}

	return segments[0], port, namespace, nil
}

func (c *Cache) unsupportedLocationFormat(location string) error {
	return fmt.Errorf("unsupported location format: %v, supported location format: [protocol][hostname]:[port]/[namespace]", location)
}
