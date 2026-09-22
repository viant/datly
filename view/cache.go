package view

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/viant/afs/option"
	"github.com/viant/afs/url"
	"github.com/viant/datly/internal/cache/managed"
	"github.com/viant/datly/internal/converter"
	"github.com/viant/datly/logger"
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
		// Warmups holds additional warmup definitions; the singular Warmup, when set, is always first.
		Warmups []*Warmup `json:",omitempty" yaml:",omitempty"`
		// SharedCases holds named reusable case sets referenced by Warmup.CaseRefs.
		SharedCases map[string][]*CacheParameters `json:",omitempty" yaml:",omitempty"`

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
		Name           string `json:",omitempty" yaml:",omitempty"`
		Priority       int    `json:",omitempty" yaml:",omitempty"`
		IndexColumn    string
		IndexParameter string     `json:",omitempty" yaml:",omitempty"`
		IndexMeta      bool       `json:",omitempty"`
		Limit          *int       `json:",omitempty" yaml:",omitempty"`
		MaxCases       *int       `json:",omitempty" yaml:",omitempty"`
		FieldNames     []string   `json:",omitempty" yaml:",omitempty"`
		Connector      *Connector `json:",omitempty"`
		CaseRefs       []string   `json:",omitempty" yaml:",omitempty"`
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
		Selector     *Statelet
		Column       string
		MetaColumn   string
		IndexMeta    bool
		Label        string
		FieldNames   []string
		StoredFields []ProjectionField
		// Warmup references the warmup definition this input originated from.
		Warmup *Warmup
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

func (c *Cache) init(ctx context.Context, resource *Resource, aView *View) (err error) {
	if c._initialized {
		return nil
	}

	c._initialized = true
	defer func() {
		if err != nil {
			c._initialized = false
		}
	}()
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

	if err := c.validateTTL(); err != nil {
		return fmt.Errorf("view %s cache: %w", viewName, err)
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

// validateTTL prevents provider-specific sentinel values and duration overflow.
func (c *Cache) validateTTL() error {
	if c.TimeToLiveMs <= 0 || uint64(c.TimeToLiveMs) > uint64((1<<63-1)/int64(time.Millisecond)) {
		return fmt.Errorf("TimeToLiveMs must be a positive representable duration")
	}
	if url.Scheme(c.Provider, "") == aerospikeType {
		// Aerospike reserves the largest two uint32 values and zero.
		if c.TimeToLiveMs%1000 != 0 || uint64(c.TimeToLiveMs/1000) >= uint64(^uint32(0)-1) {
			return fmt.Errorf("Aerospike TimeToLiveMs must be whole positive seconds below 4294967294")
		}
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

		owner := c.cacheOwner(aView)
		store := managed.NewFileStore(strings.TrimRight(expandedLoc, "/") + "/.datly-generations/" + owner)
		service := managed.New(afsCache, store, owner, c.creationObserver(aView))
		return func() (cache.Cache, error) { return service, nil }, nil
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
		SocketTimeoutMs:       c.AerospikeConfig.SocketTimeoutInMs,
		SleepBetweenRetriesMs: c.SleepBetweenRetriesInMs,
	}

	var resetTimout *time.Duration
	if c.AerospikeConfig.ResetFailuresInMs != 0 {
		resetDuration := time.Duration(c.AerospikeConfig.ResetFailuresInMs) * time.Millisecond
		resetTimout = &resetDuration
	}

	failureHandler := aerospike.NewFailureHandler(int64(c.AerospikeConfig.FailedRequestLimit), resetTimout)

	return func() (cache.Cache, error) {
		client, err := clientProvider()
		if err != nil {
			return nil, err
		}

		native, err := aerospike.New(namespace, expanded, client, uint32(c.TimeToLiveMs/1000), timeoutConfig, failureHandler)
		if err != nil {
			return nil, err
		}
		owner := c.cacheOwner(aView)
		store, err := managed.NewAerospikeStore(client, namespace, expanded, owner)
		if err != nil {
			return nil, err
		}
		return managed.New(native, store, owner, c.creationObserver(aView)), nil
	}, nil
}

// cacheOwner scopes shared storage to a resource/view/connector identity.
func (c *Cache) cacheOwner(aView *View) string {
	source := ""
	if resource := aView.GetResource(); resource != nil {
		source = resource.SourceURL
	}
	// Deployment roots may differ between replicas; route-relative identity is stable.
	if i := strings.Index(source, "/routes/"); i >= 0 {
		source = source[i+len("/routes/"):]
	}
	connector := ""
	if aView.Connector != nil {
		data, _ := json.Marshal(aView.Connector.DBConfig)
		connector = string(data)
	}
	data, _ := json.Marshal([]string{source, aView.Name, connector})
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

// InvalidateCache retires cache publications without deleting another view's data.
func (c *Cache) InvalidateCache(ctx context.Context, scope string) (string, error) {
	selected := managed.Scope(scope)
	if err := selected.Validate(); err != nil {
		return "", err
	}
	if c.newCache == nil {
		return "", fmt.Errorf("cache is not initialized")
	}
	service, err := c.Service()
	if err != nil {
		return "", err
	}
	invalidator, ok := service.(interface {
		Invalidate(context.Context, managed.Scope) (string, error)
	})
	if !ok {
		return "", fmt.Errorf("cache provider does not support scoped invalidation")
	}
	return invalidator.Invalidate(ctx, selected)
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
	location := strings.ReplaceAll(c.Location, `${View\.`, `${View.`)
	expanded := locationMap.ExpandAsText(location)
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

	if c.Warmup == nil && len(c.Warmups) == 0 {
		c.Warmup = source.Warmup.clone()
		for _, item := range source.Warmups {
			c.Warmups = append(c.Warmups, item.clone())
		}
	}

	if c.SharedCases == nil && len(source.SharedCases) > 0 {
		c.SharedCases = cloneSharedCases(source.SharedCases)
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
		SharedCases:     cloneSharedCases(c.SharedCases),
	}
	for _, item := range c.Warmups {
		cloned.Warmups = append(cloned.Warmups, item.clone())
	}

	return cloned
}

func cloneSharedCases(caseSets map[string][]*CacheParameters) map[string][]*CacheParameters {
	if caseSets == nil {
		return nil
	}
	cloned := make(map[string][]*CacheParameters, len(caseSets))
	for name, cases := range caseSets {
		items := make([]*CacheParameters, 0, len(cases))
		for _, item := range cases {
			items = append(items, item.clone())
		}
		cloned[name] = items
	}
	return cloned
}

func (w *Warmup) clone() *Warmup {
	if w == nil {
		return nil
	}

	cloned := &Warmup{
		Name:           w.Name,
		Priority:       w.Priority,
		IndexColumn:    w.IndexColumn,
		IndexParameter: w.IndexParameter,
		IndexMeta:      w.IndexMeta,
		FieldNames:     append([]string(nil), w.FieldNames...),
		CaseRefs:       append([]string(nil), w.CaseRefs...),
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

// EffectiveWarmups returns a defensive ordered slice of every warmup definition:
// the singular Warmup first, followed by the plural Warmups in declaration order.
func (c *Cache) EffectiveWarmups() []*Warmup {
	if c == nil {
		return nil
	}
	var result []*Warmup
	if c.Warmup != nil {
		result = append(result, c.Warmup)
	}
	for _, item := range c.Warmups {
		if item == nil {
			continue
		}
		result = append(result, item)
	}
	return result
}

// HasWarmup reports whether the cache defines a singular or plural warmup.
func (c *Cache) HasWarmup() bool {
	return len(c.EffectiveWarmups()) > 0
}

// EffectiveName canonicalizes an absent Name from IndexParameter, falling back to IndexColumn.
func (w *Warmup) EffectiveName() string {
	if w == nil {
		return ""
	}
	if name := strings.TrimSpace(w.Name); name != "" {
		return name
	}
	if name := strings.TrimSpace(w.IndexParameter); name != "" {
		return name
	}
	return strings.TrimSpace(w.IndexColumn)
}

// GenerateCacheInput preserves the singular compatibility API: it generates inputs
// only for the singular Warmup definition.
func (c *Cache) GenerateCacheInput(ctx context.Context) ([]*CacheInput, error) {
	if c.Warmup == nil {
		return []*CacheInput{}, nil
	}
	return c.generateWarmupCacheInput(ctx, c.Warmup)
}

// GenerateCacheInputs generates inputs for every effective warmup, singular first then
// plural, in declaration order. Cases are generated per owning warmup only; no global
// cartesian product across warmups is computed. Each input carries its originating warmup.
func (c *Cache) GenerateCacheInputs(ctx context.Context) ([]*CacheInput, error) {
	var result []*CacheInput
	for _, warmup := range c.EffectiveWarmups() {
		inputs, err := c.generateWarmupCacheInput(ctx, warmup)
		if err != nil {
			return nil, err
		}
		result = append(result, inputs...)
	}
	if result == nil {
		result = []*CacheInput{}
	}
	return result, nil
}

func (c *Cache) generateWarmupCacheInput(ctx context.Context, warmup *Warmup) ([]*CacheInput, error) {
	if len(warmup.Cases) == 0 {
		input, err := c.newInputWithError(warmup, NewStatelet(), nil)
		if err != nil {
			return nil, err
		}
		if c.maxCasesExceeded(warmup, 0, 0, input) {
			if maxCases := warmupMaxCases(warmup); maxCases > 0 {
				fmt.Printf("[INFO] cache warmup selector cap view=%s max_cases=%d selected_entries=0 selected_selectors=0\n", c.owner.Name, maxCases)
			}
			return []*CacheInput{}, nil
		}
		return []*CacheInput{input}, nil
	}

	paramValues := make([][][]interface{}, len(warmup.Cases))
	results := make(chan cacheParamValuesResult, len(warmup.Cases))
	for i, dataSet := range warmup.Cases {
		go func(index int, set *CacheParameters) {
			values, err := c.generateDatasetParamValues(ctx, set)
			results <- cacheParamValuesResult{index: index, values: values, err: err}
		}(i, dataSet)
	}
	for i := 0; i < len(warmup.Cases); i++ {
		result := <-results
		if result.err != nil {
			return nil, result.err
		}
		paramValues[result.index] = result.values
	}

	var cacheInputPermutations []*CacheInput
	selectedEntries := 0
	for i, dataSet := range warmup.Cases {
		selectors, err := c.generateDatasetSelectors(warmup, dataSet, paramValues[i], selectedEntries)
		if err != nil {
			return nil, err
		}
		cacheInputPermutations = append(cacheInputPermutations, selectors...)
		selectedEntries += c.cacheInputEntryCount(selectors...)
		if maxCases := warmupMaxCases(warmup); maxCases > 0 && selectedEntries >= maxCases {
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

func (c *Cache) generateDatasetSelectors(warmup *Warmup, set *CacheParameters, availableValues [][]interface{}, selectedEntries int) ([]*CacheInput, error) {
	var result []*CacheInput
	if err := c.appendSelectors(warmup, set, availableValues, &result, selectedEntries); err != nil {
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
	if c.owner == nil {
		return nil
	}
	warmups := c.EffectiveWarmups()
	if len(warmups) == 0 {
		return nil
	}
	if err := c.validateWarmupIdentities(warmups); err != nil {
		return err
	}
	for _, warmup := range warmups {
		if err := c.initWarmupItem(ctx, resource, warmup); err != nil {
			return err
		}
	}
	return nil
}

func (c *Cache) validateWarmupIdentities(warmups []*Warmup) error {
	if len(warmups) < 2 {
		return nil
	}
	seenNames := make(map[string]bool, len(warmups))
	seenIndexes := make(map[string]bool, len(warmups))
	for _, warmup := range warmups {
		name := strings.ToLower(strings.TrimSpace(warmup.EffectiveName()))
		if seenNames[name] {
			return fmt.Errorf("duplicate warmup name %q at View %v", name, c.owner.Name)
		}
		seenNames[name] = true

		indexIdentity := strings.ToLower(strings.Join([]string{strings.TrimSpace(warmup.IndexColumn), strings.TrimSpace(warmup.IndexParameter)}, "|"))
		if seenIndexes[indexIdentity] {
			return fmt.Errorf("duplicate warmup index identity %q (indexColumn|indexParameter) at View %v", indexIdentity, c.owner.Name)
		}
		seenIndexes[indexIdentity] = true
	}
	return nil
}

func (c *Cache) expandWarmupCaseRefs(warmup *Warmup) error {
	if len(warmup.CaseRefs) == 0 {
		return nil
	}
	var expanded []*CacheParameters
	for _, ref := range warmup.CaseRefs {
		ref = strings.TrimSpace(ref)
		cases, ok := c.SharedCases[ref]
		if !ok {
			return fmt.Errorf("not found warmup case set %q referenced by warmup %v at View %v", ref, warmup.EffectiveName(), c.owner.Name)
		}
		for _, item := range cases {
			expanded = append(expanded, item.clone())
		}
	}
	warmup.Cases = dedupeWarmupCases(append(expanded, warmup.Cases...))
	return nil
}

// dedupeWarmupCases removes duplicated cases within one warmup by canonical parameter
// name and value; it also makes repeated CaseRefs expansion idempotent.
func dedupeWarmupCases(cases []*CacheParameters) []*CacheParameters {
	result := make([]*CacheParameters, 0, len(cases))
	seen := make(map[string]bool, len(cases))
	for _, item := range cases {
		key := warmupCaseKey(item)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, item)
	}
	return result
}

func warmupCaseKey(item *CacheParameters) string {
	if item == nil {
		return ""
	}
	builder := strings.Builder{}
	for _, paramValue := range item.Set {
		if paramValue == nil {
			continue
		}
		builder.WriteString(strings.TrimSpace(paramValue.Name))
		builder.WriteString("=")
		builder.WriteString(fmt.Sprint(paramValue.Values...))
		builder.WriteString(fmt.Sprintf(";excludeDefault=%v|", paramValue.ExcludeDefault))
	}
	builder.WriteString("fields=")
	builder.WriteString(strings.Join(item.FieldNames, ","))
	return builder.String()
}

func (c *Cache) initWarmupItem(ctx context.Context, resource *Resource, warmup *Warmup) error {
	if err := c.expandWarmupCaseRefs(warmup); err != nil {
		return err
	}

	c.addNonRequiredWarmupIfNeeded(warmup)

	_, ok := c.owner.ColumnByName(warmup.IndexColumn)
	if !ok && warmup.IndexColumn != "" {
		return fmt.Errorf("not found warmup column %v at View %v", warmup, c.owner.Name)
	}

	for _, dataset := range warmup.Cases {

		for _, paramValue := range dataset.Set {
			if err := c.ensureParam(paramValue); err != nil {
				return err
			}
		}
	}

	if warmup.Connector != nil {
		if err := warmup.Connector.Init(ctx, resource.GetConnectors()); err != nil {
			return err
		}
	}

	if err := c.validateWarmupFieldNames(warmup.FieldNames); err != nil {
		return err
	}
	if err := c.validateWarmupBudget("maxCases", warmup.MaxCases); err != nil {
		return err
	}
	for _, dataset := range warmup.Cases {
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

func (c *Cache) addNonRequiredWarmupIfNeeded(warmup *Warmup) {
	if len(warmup.Cases) != 0 {
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

	warmup.Cases = append(warmup.Cases, &CacheParameters{
		Set: values,
	})
}

func (c *Cache) appendSelectors(warmup *Warmup, set *CacheParameters, paramValues [][]interface{}, selectors *[]*CacheInput, selectedEntries int) error {
	for i, value := range paramValues {
		if len(value) == 0 {
			return fmt.Errorf("parameter %v is required but there was no data", set.Set[i].Name)
		}
	}

	indexes := make([]int, len(paramValues))
	generatedEntries := 0
	if len(indexes) == 0 {
		input, err := c.newInputWithError(warmup, NewStatelet(), set)
		if err != nil {
			return err
		}
		if c.maxCasesExceeded(warmup, selectedEntries, generatedEntries, input) {
			return nil
		}
		*selectors = append(*selectors, input)
		generatedEntries += c.cacheInputEntryCount(input)
		fmt.Printf("[INFO] cache warmup selector view=%s index_column=%s params= field_names=%s\n", c.owner.Name, warmup.IndexColumn, strings.Join(input.FieldNames, ","))
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
		input, err := c.newInputWithError(warmup, selector, set)
		if err != nil {
			return err
		}
		input.Label = label
		if c.maxCasesExceeded(warmup, selectedEntries, generatedEntries, input) {
			return nil
		}
		*selectors = append(*selectors, input)
		generatedEntries += c.cacheInputEntryCount(input)
		fmt.Printf("[INFO] cache warmup selector view=%s index_column=%s params=%s field_names=%s\n", c.owner.Name, warmup.IndexColumn, label, strings.Join(input.FieldNames, ","))

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
	return c.newInput(c.Warmup, selector, nil)
}

func (c *Cache) newInput(warmup *Warmup, selector *Statelet, set *CacheParameters) *CacheInput {
	input, err := c.newInputWithError(warmup, selector, set)
	if err == nil {
		return input
	}
	if c != nil && c.owner != nil {
		fmt.Printf("[INFO] cache warmup projection metadata error view=%s field_names=%v error=%v\n", c.owner.Name, fieldNamesFor(warmup, set), err)
	}
	return c.newInputWithoutStoredFields(warmup, selector, set)
}

func (c *Cache) newInputWithError(warmup *Warmup, selector *Statelet, set *CacheParameters) (*CacheInput, error) {
	fieldNames := fieldNamesFor(warmup, set)
	if selector != nil && warmup != nil && warmup.Limit != nil {
		selector.Limit = *warmup.Limit
		selector.WarmupNoLimit = *warmup.Limit == 0
	}
	c.applyWarmupFieldNames(selector, fieldNames)
	storedFields, err := ProjectionFieldsForNames(c.owner, fieldNames)
	if err != nil {
		return nil, err
	}
	input := c.newInputWithoutStoredFields(warmup, selector, set)
	input.StoredFields = append([]ProjectionField(nil), storedFields...)
	return input, nil
}

func (c *Cache) newInputWithoutStoredFields(warmup *Warmup, selector *Statelet, set *CacheParameters) *CacheInput {
	fieldNames := fieldNamesFor(warmup, set)
	indexColumn := ""
	indexMeta := false
	if warmup != nil {
		indexColumn = warmup.IndexColumn
		indexMeta = warmup.IndexMeta
	}
	return &CacheInput{
		Selector:     selector,
		Column:       indexColumn,
		MetaColumn:   indexColumn,
		IndexMeta:    (indexMeta || indexColumn != "") && c.owner.Template.Summary != nil,
		FieldNames:   append([]string(nil), fieldNames...),
		StoredFields: nil,
		Warmup:       warmup,
	}
}

func fieldNamesFor(warmup *Warmup, set *CacheParameters) []string {
	if set != nil && len(set.FieldNames) > 0 {
		return set.FieldNames
	}
	if warmup == nil {
		return nil
	}
	return warmup.FieldNames
}

func (c *Cache) WarmupFieldNamesForSelector(selector *Statelet) ([]string, bool) {
	if c == nil {
		return nil, true
	}
	return c.WarmupFieldNames(c.Warmup, selector)
}

// WarmupFieldNames resolves projection field names for the given warmup and selector.
func (c *Cache) WarmupFieldNames(warmup *Warmup, selector *Statelet) ([]string, bool) {
	if c == nil || warmup == nil {
		return nil, true
	}
	matchedAny := false
	var matchedColumns []string
	for _, candidate := range warmup.Cases {
		if candidate == nil || len(candidate.FieldNames) == 0 || !c.warmupCaseMatchesSelector(candidate, selector) {
			continue
		}
		columns, ok := c.warmupProjectionColumns(candidate.FieldNames)
		if !ok {
			return nil, false
		}
		if !matchedAny {
			matchedAny = true
			matchedColumns = columns
			continue
		}
		if !stringSlicesEqual(matchedColumns, columns) {
			return nil, false
		}
	}
	if matchedAny {
		return matchedColumns, true
	}
	return warmup.FieldNames, true
}

func (c *Cache) warmupProjectionColumns(fieldNames []string) ([]string, bool) {
	if len(fieldNames) == 0 {
		return nil, true
	}
	if c == nil || c.owner == nil {
		return fieldNames, true
	}
	columns, err := ProjectionColumnsForNames(c.owner, fieldNames)
	if err == nil {
		return columns, true
	}
	columns = make([]string, 0, len(fieldNames))
	for _, fieldName := range fieldNames {
		column, ok := c.owner.ColumnByName(fieldName)
		if !ok {
			column, ok = c.warmupColumnByNormalizedName(fieldName)
		}
		if !ok {
			return nil, false
		}
		columns = append(columns, column.Name)
	}
	return columns, true
}

func (c *Cache) warmupColumnByNormalizedName(name string) (*Column, bool) {
	if c == nil || c.owner == nil {
		return nil, false
	}
	normalized := normalizeProjectionFieldName(name)
	for _, column := range c.owner.Columns {
		if column == nil {
			continue
		}
		if normalizeProjectionFieldName(column.Name) == normalized ||
			normalizeProjectionFieldName(column.FieldName()) == normalized ||
			normalizeProjectionFieldName(column.DatabaseColumn) == normalized {
			return column, true
		}
	}
	return nil, false
}

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if normalizeProjectionFieldName(left[i]) != normalizeProjectionFieldName(right[i]) {
			return false
		}
	}
	return true
}

func (c *Cache) warmupCaseMatchesSelector(candidate *CacheParameters, selector *Statelet) bool {
	if candidate == nil || selector == nil || selector.Template == nil {
		return false
	}
	for _, paramValue := range candidate.Set {
		if paramValue == nil {
			return false
		}
		actual, ok := warmupSelectorValue(selector, paramValue)
		if !ok {
			return false
		}
		candidates := paramValue.Values
		if paramValue._param != nil {
			var err error
			candidates, err = c.getParamValues(context.Background(), paramValue)
			if err != nil {
				return false
			}
		}
		if !warmupValueMatches(actual, candidates) {
			return false
		}
	}
	return true
}

func warmupSelectorValue(selector *Statelet, paramValue *ParamValue) (interface{}, bool) {
	if selector == nil || selector.Template == nil || paramValue == nil {
		return nil, false
	}
	if paramValue._param != nil && paramValue._param.Selector() != nil {
		stateSelector := paramValue._param.Selector()
		if !stateSelector.Has(selector.Template.Pointer()) {
			return nil, true
		}
		return stateSelector.Value(selector.Template.Pointer()), true
	}
	stateSelector, err := selector.Template.Selector(paramValue.Name)
	if err != nil || stateSelector == nil {
		return nil, false
	}
	if !stateSelector.Has(selector.Template.Pointer()) {
		return nil, true
	}
	return stateSelector.Value(selector.Template.Pointer()), true
}

func warmupValueMatches(actual interface{}, candidates []interface{}) bool {
	if len(candidates) == 0 {
		return actual == nil || reflect.ValueOf(actual).IsZero()
	}
	for _, candidate := range candidates {
		if reflect.DeepEqual(actual, candidate) || fmt.Sprint(actual) == fmt.Sprint(candidate) {
			return true
		}
	}
	return false
}

func warmupMaxCases(warmup *Warmup) int {
	if warmup == nil || warmup.MaxCases == nil || *warmup.MaxCases <= 0 {
		return 0
	}
	return *warmup.MaxCases
}

func (c *Cache) maxCasesExceeded(warmup *Warmup, selectedEntries, generatedEntries int, input *CacheInput) bool {
	maxCases := warmupMaxCases(warmup)
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
	selector.columnNamesMu.Lock()
	defer selector.columnNamesMu.Unlock()
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
		if selector._columnNames[columnName] || selector._columnNames[outputName] {
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

func (c *Cache) creationObserver(v *View) func(string, int) {
	return func(kind string, entries int) {
		logger.IncrementValueBy(v.Counter, cacheCreatedMetric, int64(entries))
		metric := cacheLazyCreatedMetric
		if kind == string(managed.Warmup) {
			metric = cacheWarmupCreatedMetric
		}
		logger.IncrementValueBy(v.Counter, metric, int64(entries))
	}
}
