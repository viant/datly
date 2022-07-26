package view

import (
	"context"
	"encoding/json"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/aerospike"
	"github.com/viant/sqlx/io/read/cache/afs"
	rdata "github.com/viant/toolbox/data"
	"strconv"
	"strings"
	"time"
)

type (
	Cache struct {
		Location     string
		Provider     string
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

func (c *Cache) init(ctx context.Context, aView *View) error {
	if c.initialized {
		return nil
	}
	c.initialized = true
	viewName := aView.Name

	if c.Location == "" {
		return fmt.Errorf("view %v cache Location can't be empty", viewName)
	}

	if c.TimeToLiveMs == 0 {
		return fmt.Errorf("view %v cache TimeToLiveMs can't be empty", viewName)
	}

	aCache, err := c.cacheService(aView)
	if err != nil {
		return fmt.Errorf("view %v error: %w", viewName, err)
	}

	c.cache = aCache
	return nil
}

func (c *Cache) cacheService(aView *View) (cache.Cache, error) {
	scheme := url.Scheme(c.Provider, "")
	switch scheme {
	case aerospikeType:
		return c.aerospikeCache(aView)
	default:
		return afs.NewCache(c.Location, time.Duration(c.TimeToLiveMs)*time.Millisecond, aView.Name, option.NewStream(c.PartSize, 0))
	}
}

func (c *Cache) aerospikeCache(aView *View) (cache.Cache, error) {
	if c.Location == "" {
		return nil, fmt.Errorf("aerospike cache SetName cannot be empty")
	}

	host, port, namespace, err := c.split(c.Provider)
	if err != nil {
		return nil, err
	}

	client, err := as.NewClient(host, port)
	if err != nil {
		return nil, err
	}

	viewParam := asViewParam(aView)
	asBytes, err := json.Marshal(viewParam)
	if err != nil {
		return nil, err
	}

	locationMap := &rdata.Map{}

	viewMap := map[string]interface{}{}
	if err = json.Unmarshal(asBytes, &viewMap); err != nil {
		return nil, err
	}

	locationMap.Put("view", viewMap)

	expanded := locationMap.ExpandAsText(c.Location)
	return aerospike.New(namespace, expanded, client, uint32(time.Duration(c.TimeToLiveMs)*time.Second/time.Millisecond))
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
