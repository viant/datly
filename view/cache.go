package view

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/datly/internal/converter"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view/state"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/aerospike"
	"github.com/viant/sqlx/io/read/cache/afs"
	"github.com/viant/tagly/format"
	rdata "github.com/viant/toolbox/data"
	"reflect"
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
		PartSize     int `json:",omitempty"`
		AerospikeConfig
		Warmup *Warmup `json:",omitempty" yaml:",omitempty"`

		newCache     func() (cache.Cache, error)
		_initialized bool
		mux          sync.Mutex
	}

	Caches []*Cache

	AerospikeConfig struct {
		SleepBetweenRetriesInMs int `json:",omitempty"`
		MaxRetries              int `json:",omitempty"`
		TotalTimeoutInMs        int `json:",omitempty"`
		SocketTimeoutInMs       int `json:",omitempty"`
		FailedRequestLimit      int `json:",omitempty"`
		ResetFailuresInMs       int `json:",omitempty"`
	}

	Warmup struct {
		IndexColumn    string
		IndexParameter string     `json:",omitempty" yaml:",omitempty"`
		IndexMeta      bool       `json:",omitempty"`
		Limit          *int       `json:",omitempty" yaml:",omitempty"`
		MaxCases       *int       `json:",omitempty" yaml:",omitempty"`
		FieldNames     []string   `json:",omitempty" yaml:",omitempty"`
		Connector      *Connector `json:",omitempty"`
		Cases          []*CacheParameters
	}

	CacheParameters struct {
		Set        []*ParamValue
		FieldNames []string `json:",omitempty" yaml:",omitempty"`
	}

	ParamValue struct {
		Name   string
		Values []interface{}
		// ExcludeDefault keeps explicitly declared warmup cases from adding an extra nil/default selector.
		ExcludeDefault bool `json:",omitempty" yaml:",omitempty"`

		_param        *state.Parameter
		_location     *time.Location
		_locationInit bool
	}

	CacheInput struct {
		Selector   *Statelet
		Column     string
		MetaColumn string
		IndexMeta  bool
		Label      string
		FieldNames []string
	}

	CacheInputFn func() ([]*CacheInput, error)

	cacheParamValuesResult struct {
		index  int
		values [][]interface{}
		err    error
	}
)

const (
	defaultType   = ""
	afsType       = "afs"
	aerospikeType = "aerospike"
)

var warmupNow = time.Now

func (c Caches) Has(name string) bool {
	for _, candidate := range c {
		if candidate.Name == name {
			return true
		}
	}
	return false
}

func (r *Caches) Append(cache *Cache) {
	if r.Has(cache.Name) {
		return
	}
	*r = append(*r, cache)
}

func (c *Cache) init(ctx context.Context, resource *Resource, aView *View) error {
	if c._initialized {
		return nil
	}

	c._initialized = true
	c.owner = aView
	var viewName string
	if aView != nil {
		viewName = aView.Name
	}

	if err := c.inheritIfNeeded(ctx, resource, aView); err != nil {
		return err
	}

	if c.Location == "" {
		return fmt.Errorf("View %v cache State can't be empty", viewName)
	}

	if c.TimeToLiveMs == 0 {
		return fmt.Errorf("View %v cache TimeToLiveMs can't be empty", viewName)
	}

	//if c.ErrorTimeToLiveMs == 0 {
	//	return fmt.Errorf("View %v cache ErrorTimeToLiveMs can't be empty", viewName)
	//}

	if err := c.ensureCacheClient(aView, viewName); err != nil {
		return err
	}

	if err := c.initWarmup(ctx, resource); err != nil {
		return err
	}

	return nil
}

func (c *Cache) ensureCacheClient(aView *View, viewName string) error {
	if c.newCache != nil {
		return nil
	}

	if aView == nil {
		return nil
	}

	var err error
	c.newCache, err = c.cacheService(viewName, aView)
	if err != nil {
		return err
	}

	return nil
}

func (c *Cache) cacheService(name string, aView *View) (func() (cache.Cache, error), error) {
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

		afsCache, err := afs.NewCache(expandedLoc, time.Duration(c.TimeToLiveMs)*time.Millisecond, aView.Name, option.NewStream(c.PartSize, 0))
		if err != nil {
			return nil, err
		}

		return func() (cache.Cache, error) {
			return afsCache, nil
		}, nil
	}
}

func (c *Cache) aerospikeCache(aView *View) (func() (cache.Cache, error), error) {
	if c.Location == "" {
		return nil, fmt.Errorf("aerospike cache SetName cannot be empty")
	}

	host, port, namespace, err := c.split(c.Provider)
	if err != nil {
		return nil, err
	}

	clientProvider := aClientPool.Client(host, port)

	expanded, err := c.expandLocation(aView)
	if err != nil {
		return nil, err
	}

	timeoutConfig := &aerospike.TimeoutConfig{
		MaxRetries:            c.AerospikeConfig.MaxRetries,
		TotalTimeoutMs:        c.AerospikeConfig.TotalTimeoutInMs,
		SleepBetweenRetriesMs: c.SleepBetweenRetriesInMs,
	}

	var resetTimout *time.Duration
	if c.AerospikeConfig.ResetFailuresInMs != 0 {
		resetDuration := time.Duration(c.AerospikeConfig.ResetFailuresInMs)
		resetTimout = &resetDuration
	}

	failureHandler := aerospike.NewFailureHandler(int64(c.AerospikeConfig.FailedRequestLimit), resetTimout)

	return func() (cache.Cache, error) {
		client, err := clientProvider()
		if err != nil {
			return nil, err
		}

		return aerospike.New(namespace, expanded, client, uint32(c.TimeToLiveMs/1000), timeoutConfig, failureHandler)
	}, nil
}

func (c *Cache) expandLocation(aView *View) (string, error) {
	viewParam := AsViewParam(aView, nil, nil)
	asBytes, err := json.Marshal(viewParam)
	if err != nil {
		return "", err
	}

	locationMap := &rdata.Map{}

	viewMap := map[string]interface{}{}
	if err = json.Unmarshal(asBytes, &viewMap); err != nil {
		return "", err
	}

	locationMap.Put("View", viewMap)
	expanded := locationMap.ExpandAsText(c.Location)
	return expanded, nil
}

func (c *Cache) ExpandedLocation(aView *View) (string, error) {
	if c == nil {
		return "", nil
	}
	return c.expandLocation(aView)
}

func (c *Cache) Service() (cache.Cache, error) {
	return c.newCache()
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
		return "", 0, "", fmt.Errorf("invalid cache: %w", err)
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

func (c *Cache) cloneForInheritance() *Cache {
	if c == nil {
		return nil
	}

	cloned := &Cache{
		Reference:       c.Reference,
		Name:            c.Name,
		Location:        c.Location,
		Provider:        c.Provider,
		TimeToLiveMs:    c.TimeToLiveMs,
		PartSize:        c.PartSize,
		AerospikeConfig: c.AerospikeConfig,
		Warmup:          c.Warmup.clone(),
	}

	return cloned
}

func (w *Warmup) clone() *Warmup {
	if w == nil {
		return nil
	}

	cloned := &Warmup{
		IndexColumn:    w.IndexColumn,
		IndexParameter: w.IndexParameter,
		IndexMeta:      w.IndexMeta,
		FieldNames:     append([]string(nil), w.FieldNames...),
		Cases:          make([]*CacheParameters, 0, len(w.Cases)),
	}
	if w.Limit != nil {
		limit := *w.Limit
		cloned.Limit = &limit
	}
	if w.MaxCases != nil {
		maxCases := *w.MaxCases
		cloned.MaxCases = &maxCases
	}
	cloned.Connector = w.Connector.clone()
	for _, item := range w.Cases {
		cloned.Cases = append(cloned.Cases, item.clone())
	}

	return cloned
}

func (c *CacheParameters) clone() *CacheParameters {
	if c == nil {
		return nil
	}

	cloned := &CacheParameters{
		FieldNames: append([]string(nil), c.FieldNames...),
		Set:        make([]*ParamValue, 0, len(c.Set)),
	}
	for _, item := range c.Set {
		cloned.Set = append(cloned.Set, item.clone())
	}

	return cloned
}

func (p *ParamValue) clone() *ParamValue {
	if p == nil {
		return nil
	}

	cloned := &ParamValue{
		Name:           p.Name,
		ExcludeDefault: p.ExcludeDefault,
	}
	cloned.Values = append([]interface{}(nil), p.Values...)

	return cloned
}

func (c *Cache) GenerateCacheInput(ctx context.Context) ([]*CacheInput, error) {
	if len(c.Warmup.Cases) == 0 {
		input := c.NewInput(NewStatelet())
		if c.maxCasesExceeded(0, 0, input) {
			if maxCases := c.maxCases(); maxCases > 0 {
				fmt.Printf("[INFO] cache warmup selector cap view=%s max_cases=%d selected_entries=0 selected_selectors=0\n", c.owner.Name, maxCases)
			}
			return []*CacheInput{}, nil
		}
		return []*CacheInput{input}, nil
	}

	paramValues := make([][][]interface{}, len(c.Warmup.Cases))
	results := make(chan cacheParamValuesResult, len(c.Warmup.Cases))
	for i, dataSet := range c.Warmup.Cases {
		go func(index int, set *CacheParameters) {
			values, err := c.generateDatasetParamValues(ctx, set)
			results <- cacheParamValuesResult{index: index, values: values, err: err}
		}(i, dataSet)
	}
	for i := 0; i < len(c.Warmup.Cases); i++ {
		result := <-results
		if result.err != nil {
			return nil, result.err
		}
		paramValues[result.index] = result.values
	}

	var cacheInputPermutations []*CacheInput
	selectedEntries := 0
	for i, dataSet := range c.Warmup.Cases {
		selectors, err := c.generateDatasetSelectors(dataSet, paramValues[i], selectedEntries)
		if err != nil {
			return nil, err
		}
		cacheInputPermutations = append(cacheInputPermutations, selectors...)
		selectedEntries += c.cacheInputEntryCount(selectors...)
		if maxCases := c.maxCases(); maxCases > 0 && selectedEntries >= maxCases {
			fmt.Printf("[INFO] cache warmup selector cap view=%s max_cases=%d selected_entries=%d selected_selectors=%d\n", c.owner.Name, maxCases, selectedEntries, len(cacheInputPermutations))
			break
		}
	}

	return cacheInputPermutations, nil
}

func (c *Cache) generateDatasetParamValues(ctx context.Context, set *CacheParameters) ([][]interface{}, error) {
	var availableValues [][]interface{}

	for i := range set.Set {
		paramValues, err := c.getParamValues(ctx, set.Set[i])
		if err != nil {
			return nil, err
		}

		availableValues = append(availableValues, paramValues)
	}

	return availableValues, nil
}

func (c *Cache) generateDatasetSelectors(set *CacheParameters, availableValues [][]interface{}, selectedEntries int) ([]*CacheInput, error) {
	var result []*CacheInput
	if err := c.appendSelectors(set, availableValues, &result, selectedEntries); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *Cache) getParamValues(ctx context.Context, paramValue *ParamValue) ([]interface{}, error) {
	result := make([]interface{}, len(paramValue.Values), len(paramValue.Values)+1)
	for i, value := range paramValue.Values {
		value = resolveWarmupValue(value, paramValue._param, paramValue._location)
		marshal := fmt.Sprintf("%v", value)
		converted, _, err := converter.Convert(marshal, paramValue._param.Schema.Type(), false, paramValue._param.DateFormat)
		if err != nil {
			return nil, fmt.Errorf("failed to convert %v, %w", paramValue.Name, err)
		}
		result[i] = converted
	}

	if !paramValue._param.IsRequired() && !paramValue.ExcludeDefault {
		result = append(result, nil)
	}

	return result, nil
}

func resolveWarmupValue(value interface{}, param *state.Parameter, location *time.Location) interface{} {
	raw, ok := value.(string)
	if !ok {
		return value
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || !strings.HasPrefix(raw, "@") {
		return value
	}
	now := warmupReferenceTime(location)
	switch strings.ToLower(raw) {
	case "@today":
		return formatWarmupDate(now, param)
	case "@yesterday":
		return formatWarmupDate(now.AddDate(0, 0, -1), param)
	default:
		return value
	}
}

func warmupReferenceTime(location *time.Location) time.Time {
	now := warmupNow()
	if location == nil {
		return now.UTC()
	}
	return now.In(location)
}

func warmupLocation(param *state.Parameter) (*time.Location, error) {
	if param == nil || strings.TrimSpace(param.Tag) == "" {
		return nil, nil
	}
	parsed, err := format.Parse(reflect.StructTag(param.Tag))
	if err != nil {
		return nil, fmt.Errorf("invalid warmup format tag on parameter %s: %w", param.Name, err)
	}
	if parsed == nil || strings.TrimSpace(parsed.Timezone) == "" {
		return nil, nil
	}
	switch timezone := strings.TrimSpace(parsed.Timezone); timezone {
	case "UTC", "utc":
		return time.UTC, nil
	default:
		location, loadErr := time.LoadLocation(timezone)
		if loadErr != nil {
			return nil, fmt.Errorf("invalid warmup timezone %q on parameter %s: %w", timezone, param.Name, loadErr)
		}
		return location, nil
	}
}

func formatWarmupDate(value time.Time, param *state.Parameter) string {
	layout := "2006-01-02"
	if param != nil && strings.TrimSpace(param.DateFormat) != "" {
		layout = param.DateFormat
	}
	return value.Format(layout)
}

func (c *Cache) initWarmup(ctx context.Context, resource *Resource) error {
	if c.owner == nil || c.Warmup == nil {
		return nil
	}

	c.addNonRequiredWarmupIfNeeded()

	_, ok := c.owner.ColumnByName(c.Warmup.IndexColumn)
	if !ok && c.Warmup.IndexColumn != "" {
		return fmt.Errorf("not found warmup column %v at View %v", c.Warmup, c.owner.Name)
	}

	for _, dataset := range c.Warmup.Cases {

		for _, paramValue := range dataset.Set {
			if err := c.ensureParam(paramValue); err != nil {
				return err
			}
		}
	}

	if c.Warmup.Connector != nil {
		if err := c.Warmup.Connector.Init(ctx, resource.GetConnectors()); err != nil {
			return err
		}
	}

	if err := c.validateWarmupFieldNames(c.Warmup.FieldNames); err != nil {
		return err
	}
	if err := c.validateWarmupBudget("maxCases", c.Warmup.MaxCases); err != nil {
		return err
	}
	for _, dataset := range c.Warmup.Cases {
		if err := c.validateWarmupFieldNames(dataset.FieldNames); err != nil {
			return err
		}
	}

	return nil
}

func (c *Cache) ensureParam(paramValue *ParamValue) error {
	param := paramValue._param
	if param == nil {
		var err error
		param, err = c.owner.Template._parametersIndex.Lookup(paramValue.Name)
		if err != nil {
			return err
		}
		paramValue._param = param
	}

	if !paramValue._locationInit {
		location, err := warmupLocation(param)
		if err != nil {
			return err
		}
		paramValue._location = location
		paramValue._locationInit = true
	}
	return nil
}

func (c *Cache) addNonRequiredWarmupIfNeeded() {
	if len(c.Warmup.Cases) != 0 {
		return
	}

	var values []*ParamValue
	for i, parameter := range c.owner.Template.Parameters {
		if parameter.IsRequired() {
			return
		}

		values = append(values, &ParamValue{Name: parameter.Name, _param: c.owner.Template.Parameters[i]})
	}

	if len(values) == 0 {
		return
	}

	c.Warmup.Cases = append(c.Warmup.Cases, &CacheParameters{
		Set: values,
	})
}

func (c *Cache) appendSelectors(set *CacheParameters, paramValues [][]interface{}, selectors *[]*CacheInput, selectedEntries int) error {
	for i, value := range paramValues {
		if len(value) == 0 {
			return fmt.Errorf("parameter %v is required but there was no data", set.Set[i].Name)
		}
	}

	indexes := make([]int, len(paramValues))
	generatedEntries := 0
	if len(indexes) == 0 {
		input := c.newInput(NewStatelet(), set)
		if c.maxCasesExceeded(selectedEntries, generatedEntries, input) {
			return nil
		}
		*selectors = append(*selectors, input)
		generatedEntries += c.cacheInputEntryCount(input)
		fmt.Printf("[INFO] cache warmup selector view=%s index_column=%s params= field_names=%s\n", c.owner.Name, c.Warmup.IndexColumn, strings.Join(input.FieldNames, ","))
		return nil
	}

outer:
	for {
		selector := &Statelet{}
		selector.Init(c.owner)
		debugParams := make([]string, 0, len(paramValues))

		for i, possibleValues := range paramValues {
			actualValue := possibleValues[indexes[i]]
			debugParams = append(debugParams, fmt.Sprintf("%s=%v", set.Set[i].Name, actualValue))
			if actualValue == nil {
				continue
			}

			if err := set.Set[i]._param.Set(selector.Template, actualValue); err != nil {
				return err
			}
		}

		label := strings.Join(debugParams, ",")
		input := c.newInput(selector, set)
		input.Label = label
		if c.maxCasesExceeded(selectedEntries, generatedEntries, input) {
			return nil
		}
		*selectors = append(*selectors, input)
		generatedEntries += c.cacheInputEntryCount(input)
		fmt.Printf("[INFO] cache warmup selector view=%s index_column=%s params=%s field_names=%s\n", c.owner.Name, c.Warmup.IndexColumn, label, strings.Join(input.FieldNames, ","))

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

func (c *Cache) NewInput(selector *Statelet) *CacheInput {
	return c.newInput(selector, nil)
}

func (c *Cache) newInput(selector *Statelet, set *CacheParameters) *CacheInput {
	fieldNames := c.fieldNamesFor(set)
	if selector != nil && c.Warmup != nil && c.Warmup.Limit != nil {
		selector.Limit = *c.Warmup.Limit
		selector.WarmupNoLimit = *c.Warmup.Limit == 0
	}
	c.applyWarmupFieldNames(selector, fieldNames)
	return &CacheInput{
		Selector:   selector,
		Column:     c.Warmup.IndexColumn,
		MetaColumn: c.Warmup.IndexColumn,
		IndexMeta:  (c.Warmup.IndexMeta || c.Warmup.IndexColumn != "") && c.owner.Template.Summary != nil,
		FieldNames: append([]string(nil), fieldNames...),
	}
}

func (c *Cache) fieldNamesFor(set *CacheParameters) []string {
	if set != nil && len(set.FieldNames) > 0 {
		return set.FieldNames
	}
	if c.Warmup == nil {
		return nil
	}
	return c.Warmup.FieldNames
}

func (c *Cache) maxCases() int {
	if c == nil || c.Warmup == nil || c.Warmup.MaxCases == nil || *c.Warmup.MaxCases <= 0 {
		return 0
	}
	return *c.Warmup.MaxCases
}

func (c *Cache) maxCasesExceeded(selectedEntries, generatedEntries int, input *CacheInput) bool {
	maxCases := c.maxCases()
	return maxCases > 0 && selectedEntries+generatedEntries+c.cacheInputEntryCount(input) > maxCases
}

func (c *Cache) cacheInputEntryCount(inputs ...*CacheInput) int {
	result := 0
	for _, input := range inputs {
		if input == nil {
			continue
		}
		result++
		if input.IndexMeta {
			result++
		}
	}
	return result
}

func (c *Cache) validateWarmupFieldNames(fieldNames []string) error {
	if len(fieldNames) == 0 {
		return nil
	}
	viewName := ""
	if c.owner != nil {
		viewName = c.owner.Name
	}
	if c.owner == nil || c.owner.Selector == nil || c.owner.Selector.Constraints == nil || !c.owner.Selector.Constraints.Projection {
		return fmt.Errorf("warmup fieldNames require projection selector on view %v", viewName)
	}
	for _, fieldName := range fieldNames {
		fieldName = strings.TrimSpace(fieldName)
		if fieldName == "" {
			return fmt.Errorf("warmup fieldNames contains empty field on view %v", viewName)
		}
		if _, ok := c.owner.ColumnByName(fieldName); !ok {
			return fmt.Errorf("not found warmup fieldName %v at View %v", fieldName, viewName)
		}
	}
	return nil
}

func (c *Cache) validateWarmupBudget(name string, value *int) error {
	if value == nil {
		return nil
	}
	if *value < 0 {
		viewName := ""
		if c.owner != nil {
			viewName = c.owner.Name
		}
		return fmt.Errorf("warmup %s must be zero or greater on view %v", name, viewName)
	}
	return nil
}

func (c *Cache) applyWarmupFieldNames(selector *Statelet, fieldNames []string) {
	if selector == nil || c.owner == nil || len(fieldNames) == 0 {
		return
	}
	if selector._columnNames == nil {
		selector._columnNames = map[string]bool{}
	}
	for _, fieldName := range fieldNames {
		fieldName = strings.TrimSpace(fieldName)
		if fieldName == "" {
			continue
		}
		column, ok := c.owner.ColumnByName(fieldName)
		if !ok {
			continue
		}
		columnName := column.Name
		outputName := column.FieldName()
		if outputName == "" {
			outputName = columnName
		}
		if selector.Has(columnName) || selector.Has(outputName) {
			continue
		}
		selector._columnNames[columnName] = true
		selector._columnNames[strings.ToLower(columnName)] = true
		selector._columnNames[outputName] = true
		selector.Columns = append(selector.Columns, columnName)
		selector.Fields = append(selector.Fields, outputName)
	}
}

func (c Caches) Unique() []*Cache {
	if len(c) == 0 {
		return []*Cache{}
	}
	var result []*Cache
	var index = make(map[string]bool, len(c))
	for i, item := range c {
		if index[item.Name] {
			continue
		}
		result = append(result, c[i])
		index[item.Name] = true
	}
	return result
}

// NewRefCache creates cache reference
func NewRefCache(name string) *Cache {
	return &Cache{Reference: shared.Reference{Ref: name}}
}
