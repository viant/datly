package view

import (
	"context"
	"encoding/json"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/datly/shared"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/aerospike"
	"github.com/viant/sqlx/io/read/cache/afs"
	rdata "github.com/viant/toolbox/data"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	Cache struct {
		shared.Reference
		owner *View

		Name         string `json:",omitempty" yaml:",omitempty"`
		Location     string
		Provider     string
		TimeToLiveMs int
		PartSize     int
		cache        cache.Cache

		initialized     bool
		aerospikeClient *as.Client
		mux             sync.Mutex
		Warmup          interface{}
	}

	CacheType string
)

const (
	defaultType   = ""
	afsType       = "afs"
	aerospikeType = "aerospike"
)

func (c *Cache) init(ctx context.Context, resource *Resource, aView *View) error {
	if c.initialized {
		return nil
	}

	c.initialized = true
	c.owner = aView
	viewName := aView.Name

	if err := c.inheritIfNeeded(ctx, resource, aView); err != nil {
		return err
	}

	if c.Location == "" {
		return fmt.Errorf("view %v cache Location can't be empty", viewName)
	}

	if c.TimeToLiveMs == 0 {
		return fmt.Errorf("view %v cache TimeToLiveMs can't be empty", viewName)
	}

	err := c.ensureCacheClient(aView, viewName)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cache) ensureCacheClient(aView *View, viewName string) error {
	if c.aerospikeClient != nil {
		return nil
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
		if aView.Name == "" {
			return nil, nil
		}
		expandedLoc, err := c.expandLocation(aView)
		if err != nil {
			return nil, err
		}
		return afs.NewCache(expandedLoc, time.Duration(c.TimeToLiveMs)*time.Millisecond, aView.Name, option.NewStream(c.PartSize, 0))
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

	client, err := aClientPool.Client(host, port)
	if err != nil {
		return nil, err
	}

	expanded, err := c.expandLocation(aView)
	if err != nil {
		return nil, err
	}

	c.aerospikeClient = client
	return aerospike.New(namespace, expanded, client, uint32(c.TimeToLiveMs/1000))
}

func (c *Cache) expandLocation(aView *View) (string, error) {
	viewParam := asViewParam(aView)
	asBytes, err := json.Marshal(viewParam)
	if err != nil {
		return "", err
	}

	locationMap := &rdata.Map{}

	viewMap := map[string]interface{}{}
	if err = json.Unmarshal(asBytes, &viewMap); err != nil {
		return "", err
	}

	locationMap.Put("view", viewMap)
	expanded := locationMap.ExpandAsText(c.Location)
	return expanded, nil
}

func (c *Cache) Service() (cache.Cache, error) {
	if c.aerospikeClient != nil {
		if err := c.recreateCacheIfNeeded(); err != nil {
			return nil, err
		}
	}

	return c.cache, nil
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

func (c *Cache) inheritIfNeeded(ctx context.Context, resource *Resource, view *View) error {
	if c.Ref == "" {
		return nil
	}

	source, ok := resource.CacheProvider(c.Ref)
	if !ok {
		return fmt.Errorf("not found cache provider with %v name", c.Ref)
	}

	if err := source.init(ctx, resource, &View{}); err != nil {
		return err
	}

	return c.inherit(source)
}

func (c *Cache) inherit(source *Cache) error {
	if c.Provider == "" {
		c.Provider = source.Provider
	}

	if c.PartSize == 0 {
		c.PartSize = source.PartSize
	}

	if c.Location == "" {
		c.Location = source.Location
	}

	if c.TimeToLiveMs == 0 {
		c.TimeToLiveMs = source.TimeToLiveMs
	}

	return nil
}

func (c *Cache) recreateCacheIfNeeded() error {
	if c.aerospikeClient.IsConnected() {
		return nil
	}

	c.mux.Lock()
	defer c.mux.Unlock()

	aerospikeCache, err := c.aerospikeCache(c.owner)
	if err != nil {
		return err
	}

	c.cache = aerospikeCache
	return err
}
