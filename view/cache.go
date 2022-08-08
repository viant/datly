package view

import (
	"context"
	"encoding/json"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/datly/converter"
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
		Warmup          *Warmup
	}

	Warmup struct {
		DataSets []*DataSet
	}

	DataSet struct {
		IndexColumn string
		ParamValues []*ParamValue
	}

	ParamValue struct {
		Name   string
		Values []interface{}

		_param *Parameter
	}

	CacheInput struct {
		Selector *Selector
		Column   string
	}

	CacheInputFn func() ([]*CacheInput, error)
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
	var viewName string
	if aView != nil {
		viewName = aView.Name
	}

	if err := c.inheritIfNeeded(ctx, resource, aView); err != nil {
		return err
	}

	if c.Location == "" {
		return fmt.Errorf("view %v cache Location can't be empty", viewName)
	}

	if c.TimeToLiveMs == 0 {
		return fmt.Errorf("view %v cache TimeToLiveMs can't be empty", viewName)
	}

	if err := c.ensureCacheClient(aView, viewName); err != nil {
		return err
	}

	if err := c.initWarmup(); err != nil {
		return err
	}

	return nil
}

func (c *Cache) ensureCacheClient(aView *View, viewName string) error {
	if c.aerospikeClient != nil {
		return nil
	}

	if aView == nil {
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

func (c *Cache) inheritIfNeeded(ctx context.Context, resource *Resource, aView *View) error {
	if c.Ref == "" {
		return nil
	}

	source, ok := resource.CacheProvider(c.Ref)
	if !ok {
		return fmt.Errorf("not found cache provider with %v name", c.Ref)
	}

	if c.Warmup == nil && source.Warmup != nil {
		warmupMarshal, err := json.Marshal(source.Warmup)
		if err != nil {
			return err
		}

		if err = json.Unmarshal(warmupMarshal, c.Warmup); err != nil {
			return err
		}
	}

	if err := source.init(ctx, resource, nil); err != nil {
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

func (c *Cache) GenerateCacheInput(ctx context.Context) ([]*CacheInput, error) {
	var cacheInputPermutations []*CacheInput
	chanSize := len(c.Warmup.DataSets)
	selectorChan := make(chan CacheInputFn, chanSize)

	for i := range c.Warmup.DataSets {
		go c.generateDatasetSelectorsChan(ctx, selectorChan, c.Warmup.DataSets[i])
	}

	counter := 0
	for selectorFn := range selectorChan {
		selectors, err := selectorFn()
		if err != nil {
			return nil, err
		}

		cacheInputPermutations = append(cacheInputPermutations, selectors...)
		counter++
		if counter == chanSize {
			close(selectorChan)
		}
	}

	return cacheInputPermutations, nil
}

func (c *Cache) generateDatasetSelectorsChan(ctx context.Context, selectorChan chan CacheInputFn, dataSet *DataSet) {
	selectors, err := c.generateDatasetSelectorsErr(ctx, dataSet)
	selectorChan <- func() ([]*CacheInput, error) {
		return selectors, err
	}
}

func (c *Cache) generateDatasetSelectorsErr(ctx context.Context, set *DataSet) ([]*CacheInput, error) {
	var availableValues [][]interface{}

	for i := range set.ParamValues {
		paramValues, err := c.getParamValues(ctx, set.ParamValues[i])
		if err != nil {
			return nil, err
		}

		availableValues = append(availableValues, paramValues)
	}

	var result []*CacheInput
	if err := c.appendSelectors(set, availableValues, &result); err != nil {
		return nil, err
	}

	return result, nil

}

func (c *Cache) getParamValues(ctx context.Context, paramValue *ParamValue) ([]interface{}, error) {
	result := make([]interface{}, len(paramValue.Values), len(paramValue.Values)+1)
	for i, value := range paramValue.Values {
		marshal, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}

		converted, _, err := converter.Convert(string(marshal), paramValue._param.Schema.Type(), paramValue._param.DateFormat)
		if err != nil {
			return nil, err
		}
		result[i] = converted
	}

	if !paramValue._param.IsRequired() {
		result = append(result, nil)
	}

	return result, nil
}

func (c *Cache) initWarmup() error {
	if c.owner == nil || c.Warmup == nil {
		return nil
	}

	for _, dataset := range c.Warmup.DataSets {

		if dataset.IndexColumn == "" {
			return fmt.Errorf("view %v warmup Column can't be empty", c.owner.Name)
		}

		_, ok := c.owner.ColumnByName(dataset.IndexColumn)
		if !ok {
			return fmt.Errorf("not found warmup column %v at view %v", dataset.IndexColumn, c.owner.Name)
		}

		for _, paramValue := range dataset.ParamValues {
			param, err := c.owner.Template._parametersIndex.Lookup(paramValue.Name)
			if err != nil {
				return err
			}

			paramValue._param = param
		}
	}

	return nil
}

func (c *Cache) appendSelectors(set *DataSet, paramValues [][]interface{}, selectors *[]*CacheInput) error {
	for i, value := range paramValues {
		if len(value) == 0 {
			return fmt.Errorf("parameter %v is required but there was no data", set.ParamValues[i].Name)
		}
	}

	indexes := make([]int, len(paramValues))

outer:
	for {
		selector := &Selector{}
		selector.Parameters.Init(c.owner)

		for i, possibleValues := range paramValues {
			actualValue := possibleValues[indexes[i]]
			if actualValue == nil {
				continue
			}

			if err := set.ParamValues[i]._param.Set(selector, actualValue); err != nil {
				return err
			}
		}

		*selectors = append(*selectors, &CacheInput{Selector: selector, Column: set.IndexColumn})

		for i := len(indexes) - 1; i >= 0; i-- {
			if indexes[i] < len(paramValues[i])-1 {
				indexes[i]++
				break
			} else {
				if i == 0 {
					break outer
				}

				indexes[i] = 0
			}
		}
	}

	return nil
}
