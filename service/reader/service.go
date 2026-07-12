package reader

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/viant/datly/internal/requesttrace"
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	vcontext "github.com/viant/datly/view/context"
	"github.com/viant/datly/view/state"
	"github.com/viant/gmetric/counter"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/structology"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xdatly/handler/exec"
	"github.com/viant/xdatly/handler/response"
)

// Service represents reader service
type Service struct {
	sqlBuilder *Builder
}

// ReadInto reads Data into provided destination, * dDest` is required. It has to be a pointer to `interface{}` or pointer to slice of `T` or `*T`
func (s *Service) ReadInto(ctx context.Context, dest interface{}, aView *view.View, opts ...Option) error {
	session, err := NewSession(dest, aView, opts...)
	if err != nil {
		return err
	}
	err = s.Read(ctx, session)
	if session.MetricPtr != nil {
		*session.MetricPtr = session.Metrics
	}
	return err
}

// Read select view from database based on View and assign it to dest. ParentDest has to be pointer.
func (s *Service) Read(ctx context.Context, session *Session) error {
	err := s.read(ctx, session)
	return err
}

func (s *Service) read(ctx context.Context, session *Session) error {
	var err error
	if err = session.Init(); err != nil {
		return err
	}
	wg := sync.WaitGroup{}
	collector := session.View.Collector(session.DataPtr, session.HandleViewMeta, session.View.MatchStrategy.ReadAll())
	errors := shared.NewErrors(0)
	s.readAll(ctx, session, collector, &wg, errors, session.Parent)
	wg.Wait()
	err = errors.Error()
	if err != nil {
		return err
	}
	collector.MergeData()
	if err = errors.Error(); err != nil {
		return err
	}
	if dest, ok := session.DataPtr.(*interface{}); ok {
		*dest = collector.Dest()
	}
	session.syncData(session.View.Schema.Cardinality)
	aState := session.State.Lookup(session.View)
	session.Filters = aState.Filters
	return nil
}

func (s *Service) afterRead(ctx context.Context, aSession *Session, collector *view.Collector, start *time.Time, executions response.SQLExecutions, err error, onFinish counter.OnDone) {
	end := Now()
	viewName := collector.View().Name
	//aSession.View.Logger.ReadTime(viewName, start, &end, err)
	elapsed := Diff(end, *start)
	metrics := &response.Metric{
		ID:         uuid.New().String(),
		StartTime:  *start,
		EndTime:    end,
		View:       viewName,
		Type:       "SELECT",
		Executions: executions,
		ElapsedMs:  int(elapsed.Milliseconds()),
		Elapsed:    elapsed.String(),
		Rows:       collector.Len(),
	}
	aSession.AddMetric(metrics)
	status := Success
	if err != nil {
		status = Error
	}
	statusText := "ok"
	if err != nil {
		statusText = "error"
	}
	onFinish(end, status)
	if aSession.DryRun {
		aSession.View.Counter.IncrementValue(status)
	}
	fmt.Printf("[INFO] datly view read reqTraceId=%s view=%s rows=%d elapsed=%s status=%s\n",
		reqTraceID(ctx),
		viewName,
		collector.Len(),
		elapsed,
		statusText)
	if value := ctx.Value(exec.ContextKey); value != nil {
		if exeCtx := value.(*exec.Context); exeCtx != nil {
			exeCtx.AppendMetrics(metrics)
		}
	}
}

func (s *Service) readAll(ctx context.Context, session *Session, collector *view.Collector, wg *sync.WaitGroup, errorCollector *shared.Errors, parent *view.View) {
	if errorCollector.Error() != nil {
		return
	}

	var collectorFetchEmitted bool
	defer s.afterReadAll(collectorFetchEmitted, collector)

	aView := collector.View()
	selector := session.State.Lookup(aView)
	if selector.Ignore {
		return
	}

	collectorChildren, err := collector.Relations(selector)
	if err != nil {
		errorCollector.Append(err)
		return
	}

	wg.Add(len(collectorChildren))
	relationGroup := sync.WaitGroup{}
	if !collector.ReadAll() {
		relationGroup.Add(len(collectorChildren))
	}

	for i := range collectorChildren {
		go func(i int, parent *view.View) {
			defer func() {
				s.afterRelationCompleted(wg, collector, &relationGroup)
			}()
			s.readAll(ctx, session, collectorChildren[i], wg, errorCollector, aView)
		}(i, aView)
	}

	collector.WaitIfNeeded()
	if err := errorCollector.Error(); err != nil {
		return
	}

	batchData := s.batchData(collector)
	if len(batchData.ColumnNames) != 0 && len(batchData.Values) == 0 && len(batchData.CompositeValues) == 0 {
		return
	}

	session.View.Counter.IncrementValue(Pending)
	defer session.View.Counter.DecrementValue(Pending)

	err = s.exhaustRead(ctx, aView, selector, batchData, collector, session)
	if err != nil {
		errorCollector.Append(err)
	}

	if collector.ReadAll() {
		return
	}

	collector.BootstrapFromParentHolder()

	collectorFetchEmitted = true
	collector.Fetched()
	relationGroup.Wait()

	onRelationerConcurrency := 1
	if aView.RelationalConcurrency != nil && aView.RelationalConcurrency.Number > 1 {
		onRelationerConcurrency = aView.RelationalConcurrency.Number
	}
	ptr, xslice := collector.Slice()
	xlen := xslice.Len(ptr)
	if onRelationerConcurrency == 1 {
		for i := 0; i < xlen; i++ {
			if actual, ok := xslice.ValuePointerAt(ptr, i).(OnRelationer); ok {
				actual.OnRelation(ctx)
				continue
			}
			break
		}
		return
	}

	// if onRelationalConcurrency > 1 , then only we call it concurrently
	concurrencyLimit := make(chan struct{}, onRelationerConcurrency)
	var onRelationWaitGroup sync.WaitGroup
	for i := 0; i < xlen; i++ {
		if actual, ok := xslice.ValuePointerAt(ptr, i).(OnRelationer); ok {
			onRelationWaitGroup.Add(1)
			concurrencyLimit <- struct{}{} // Acquire slot
			go func(actual OnRelationer) {
				defer onRelationWaitGroup.Done()
				actual.OnRelation(ctx)
				<-concurrencyLimit // Release slot
			}(actual)

			continue
		}
		break
	}
	onRelationWaitGroup.Wait()
}

func (s *Service) afterRelationCompleted(wg *sync.WaitGroup, collector *view.Collector, relationGroup *sync.WaitGroup) {
	wg.Done()
	if collector.ReadAll() {
		return
	}
	relationGroup.Done()
}

func (s *Service) afterReadAll(collectorFetchEmitted bool, collector *view.Collector) {
	if collectorFetchEmitted {
		return
	}
	collector.Unlock()
	collector.Fetched()
}

func (s *Service) batchData(collector *view.Collector) *view.BatchData {
	batchData := &view.BatchData{}
	batchData.Values, batchData.CompositeValues, batchData.ColumnNames = collector.ParentPlaceholders()
	if batchData.HasComposite() {
		batchData.ParentReadSize = len(batchData.CompositeValues)
	} else {
		batchData.ParentReadSize = len(batchData.Values)
	}
	return batchData
}

func nopCounterDone(end time.Time, values ...interface{}) int64 {
	return 0
}

func (s *Service) exhaustRead(ctx context.Context, view *view.View, selector *view.Statelet, batchData *view.BatchData, collector *view.Collector, session *Session) error {
	execution := response.SQLExecutions{}
	start := Now()
	onFinish := nopCounterDone
	if !session.DryRun {
		onFinish = view.Counter.Begin(start)
	}
	err := s.readObjects(ctx, session, batchData, view, collector, selector, &execution)
	s.afterRead(ctx, session, collector, &start, execution, err, onFinish)
	return err
}

func (s *Service) readObjects(ctx context.Context, session *Session, batchData *view.BatchData, view *view.View, collector *view.Collector, selector *view.Statelet, info *response.SQLExecutions) error {
	if batchData.HasComposite() {
		batchData.CompositeValuesBatch, batchData.Size = sliceCompositeWithLimit(batchData.CompositeValues, batchData.Size, batchData.Size+view.Batch.Size)
	} else {
		batchData.ValuesBatch, batchData.Size = sliceWithLimit(batchData.Values, batchData.Size, batchData.Size+view.Batch.Size)
	}
	visitor := collector.Visitor(ctx)
	for {
		err := s.queryInBatches(ctx, session, view, collector, visitor, info, batchData, selector)
		if err != nil {
			return err
		}
		if batchData.Size >= batchData.ParentReadSize {
			break
		}
		var nextParents int
		if batchData.HasComposite() {
			batchData.CompositeValuesBatch, nextParents = sliceCompositeWithLimit(batchData.CompositeValues, batchData.Size, batchData.Size+view.Batch.Size)
		} else {
			batchData.ValuesBatch, nextParents = sliceWithLimit(batchData.Values, batchData.Size, batchData.Size+view.Batch.Size)
		}
		batchData.Size += nextParents
	}
	return nil
}

func (s *Service) querySummary(ctx context.Context, session *Session, aView *view.View, statelet *view.Statelet, batchDataCopy *view.BatchData, collector *view.Collector, parentViewMetaParam *expand.ViewContext) (*response.SQLExecution, error) {
	selector := statelet.CloneForSummary()
	selector.Fields = []string{}
	selector.Columns = []string{}

	var indexed *cache.ParmetrizedQuery
	var cacheStats *cache.Stats
	var metaOptions []read.Option
	wg := &sync.WaitGroup{}
	wg.Add(1)

	var cacheErr error
	go func() {
		defer wg.Done()
		if !session.IsCacheEnabled(aView) {
			return
		}
		cacheMatcher, err := s.sqlBuilder.CacheMetaSQL(ctx, aView, selector, batchDataCopy, collector.Relation(), parentViewMetaParam)
		if err != nil {
			cacheErr = err
			return
		}
		cacheService, err := aView.Cache.Service()
		if err != nil {
			return
		}
		cacheStats = &cache.Stats{}
		metaOptions = []read.Option{read.WithCache(cacheService), read.WithInMatcher(cacheMatcher), read.WithCacheStats(cacheStats)}
	}()

	var err error
	indexed, err = s.sqlBuilder.ExactMetaSQL(ctx, aView, selector, batchDataCopy, collector.Relation(), parentViewMetaParam)
	if indexed == nil {
		indexed = &cache.ParmetrizedQuery{}
	}
	execInfo, onDone := NewExecutionInfo(indexed, cacheStats, collector)
	defer onDone()
	if err != nil {
		execInfo.SetError(err)
		return execInfo, err
	}
	wg.Wait()
	if cacheErr != nil {
		execInfo.SetError(cacheErr)
		return execInfo, cacheErr
	}
	db, err := aView.Db()
	if err != nil {
		execInfo.SetError(err)
		return nil, err
	}

	slice := reflect.New(aView.Template.Summary.Schema.SliceType())
	slicePtr := unsafe.Pointer(slice.Pointer())
	appender := aView.Template.Summary.Schema.Slice().Appender(slicePtr)

	SQL := indexed.SQL
	args := indexed.Args
	now := time.Now()
	defer onDone()
	reader, err := read.New(ctx, db, SQL, func() interface{} {
		add := appender.Add()
		return add
	}, metaOptions...)
	if err != nil {
		execInfo.SetError(err)
		return nil, err
	}
	defer func() {
		stmt := reader.Stmt()
		if stmt == nil {
			return
		}

		_ = stmt.Close()
	}()
	err = reader.QueryAll(ctx, func(row interface{}) error {
		return collector.AddMeta(row)
	}, args...)
	if err != nil {
		execInfo.SetError(err)
		return nil, err
	}
	finished := Now()
	aView.Logger.Log("reading view %v meta took %v, SQL: %v , Args: %v\n", aView.Name, finished.Sub(now).String(), SQL, args)
	logCacheRead(ctx, aView, cacheStats, finished.Sub(now), collector.Len(), args)
	return execInfo, nil
}

func (s *Service) buildParametrizedSQL(ctx context.Context, aView *view.View, statelet *view.Statelet, batchData *view.BatchData, collector *view.Collector, session *Session, partitions *view.Partition) (parametrizedSQL *cache.ParmetrizedQuery, columnInMatcher *cache.ParmetrizedQuery, err error) {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	relation := collector.Relation()

	var cacheErr error
	go func() {
		defer wg.Done()
		if (aView.Cache != nil && aView.Cache.Warmup != nil) || relation != nil {
			data, _ := session.ParentData()
			if aView.Cache != nil && aView.Cache.Warmup != nil {
				if relation == nil {
					columnInMatcher, cacheErr = s.topLevelWarmupMatcher(ctx, aView, statelet, data.AsParam())
					return
				}
				columnInMatcher, cacheErr = s.relationWarmupMatcher(ctx, aView, statelet, batchData, relation)
				if cacheErr != nil || columnInMatcher != nil {
					return
				}
			}
			if relation != nil {
				columnInMatcher, cacheErr = s.sqlBuilder.CacheSQLWithOptions(ctx, aView, statelet, batchData, relation, data.AsParam())
				return
			}
		}
	}()

	data, _ := session.ParentData()
	parametrizedSQL, err = s.sqlBuilder.Build(ctx, WithBuilderView(aView), WithBuilderStatelet(statelet), WithBuilderPartitions(partitions), WithBuilderBatchData(batchData), WithBuilderRelation(relation), WithBuilderExclude(
		false, relation != nil && len(batchData.ValuesBatch) > 1), WithBuilderParent(data.AsParam()))
	if err != nil {
		return nil, nil, err
	}
	wg.Wait()
	applyWarmupIdentity(parametrizedSQL, columnInMatcher)
	return parametrizedSQL, columnInMatcher, cacheErr
}

func applyWarmupIdentity(target *cache.ParmetrizedQuery, identity *cache.ParmetrizedQuery) {
	if target == nil || identity == nil {
		return
	}
	if identity.IdentitySQL != "" {
		target.IdentitySQL = identity.IdentitySQL
		target.IdentityArgs = append([]interface{}{}, identity.IdentityArgs...)
		return
	}
	target.IdentitySQL = identity.SQL
	target.IdentityArgs = append([]interface{}{}, identity.Args...)
}

func (s *Service) relationWarmupMatcher(ctx context.Context, aView *view.View, statelet *view.Statelet, batchData *view.BatchData, relation *view.Relation) (*cache.ParmetrizedQuery, error) {
	if aView == nil || aView.Cache == nil || aView.Cache.Warmup == nil || batchData == nil || relation == nil || relation.Of == nil || len(relation.Of.On) != 1 {
		return nil, nil
	}
	indexColumn := strings.TrimSpace(aView.Cache.Warmup.IndexColumn)
	if indexColumn == "" || len(batchData.ValuesBatch) == 0 || batchData.HasComposite() || len(batchData.ColumnNames) != 1 {
		return nil, nil
	}
	if !matchesWarmupIndex(aView, indexColumn, relation.Of.On[0], batchData.ColumnNames[0]) {
		return nil, nil
	}
	matcher, err := s.warmupMatcher(ctx, aView, statelet, nil)
	if err != nil || matcher == nil {
		return matcher, err
	}
	matcher.By = warmupMarkerColumn(indexColumn, relation, batchData)
	matcher.In = batchData.ValuesBatch
	return matcher, nil
}

func warmupMarkerColumn(indexColumn string, relation *view.Relation, batchData *view.BatchData) string {
	if column := normalizeWarmupColumnName(indexColumn); column != "" {
		return column
	}
	if batchData != nil && len(batchData.ColumnNames) > 0 {
		if column := normalizeWarmupColumnName(batchData.ColumnNames[0]); column != "" {
			return column
		}
	}
	if relation != nil && relation.Of != nil && len(relation.Of.On) > 0 && relation.Of.On[0] != nil {
		if column := normalizeWarmupColumnName(relation.Of.On[0].Column); column != "" {
			return column
		}
	}
	return normalizeWarmupColumnName(indexColumn)
}

func matchesWarmupIndex(aView *view.View, indexColumn string, link *view.Link, batchColumn string) bool {
	if matchesWarmupIndexColumn(indexColumn, link, batchColumn) {
		return true
	}
	if aView == nil || link == nil {
		return false
	}
	if !strings.EqualFold(normalizeWarmupColumnName(batchColumn), normalizeWarmupColumnName(link.Column)) {
		return false
	}
	warmupField := warmupIndexFieldName(indexColumn)
	if warmupField == "" {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(link.Field), strings.TrimSpace(warmupField))
}

var warmupFieldAliasPattern = regexp.MustCompile(`[^a-zA-Z0-9]+`)

func warmupIndexFieldName(indexColumn string) string {
	normalized := strings.TrimSpace(normalizeWarmupColumnName(indexColumn))
	if normalized == "" {
		return ""
	}
	parts := warmupFieldAliasPattern.Split(strings.ToLower(normalized), -1)
	builder := strings.Builder{}
	for _, part := range parts {
		if part == "" {
			continue
		}
		builder.WriteString(strings.ToUpper(part[:1]))
		if len(part) > 1 {
			builder.WriteString(part[1:])
		}
	}
	return builder.String()
}

func matchesWarmupIndexColumn(indexColumn string, link *view.Link, batchColumn string) bool {
	if link == nil {
		return false
	}
	relationColumn := strings.TrimSpace(link.Column)
	if relationColumn == "" {
		return false
	}
	return strings.EqualFold(normalizeWarmupColumnName(relationColumn), normalizeWarmupColumnName(indexColumn)) &&
		strings.EqualFold(normalizeWarmupColumnName(batchColumn), normalizeWarmupColumnName(indexColumn))
}

func normalizeWarmupColumnName(input string) string {
	input = strings.TrimSpace(input)
	if index := strings.LastIndex(input, "."); index != -1 {
		input = input[index+1:]
	}
	return strings.TrimSpace(input)
}

func cloneStructologyState(src *structology.State) *structology.State {
	if src == nil {
		return nil
	}
	cloned := src.Type().NewState()
	dstStatePtr := reflect.ValueOf(cloned.StatePtr())
	srcType := src.Type().Type()
	if dstStatePtr.IsValid() {
		if srcType.Kind() == reflect.Ptr {
			srcStatePtr := reflect.ValueOf(src.StatePtr())
			if srcStatePtr.IsValid() && srcStatePtr.Kind() == reflect.Ptr && !srcStatePtr.IsNil() {
				dstStatePtr.Elem().Set(srcStatePtr.Elem())
			}
		} else {
			currentValue := reflect.NewAt(srcType, src.Pointer()).Elem()
			dstStatePtr.Elem().Set(currentValue)
		}
	}
	if holder := src.MarkerHolder(); holder != nil {
		holderVal := reflect.ValueOf(holder)
		if holderVal.IsValid() && holderVal.Kind() == reflect.Ptr && !holderVal.IsNil() {
			holderCopy := reflect.New(holderVal.Elem().Type())
			holderCopy.Elem().Set(holderVal.Elem())
			if dstStatePtr.IsValid() && dstStatePtr.Kind() == reflect.Ptr && !dstStatePtr.IsNil() {
				hasField := dstStatePtr.Elem().FieldByName("Has")
				if hasField.IsValid() && hasField.CanSet() {
					hasField.Set(holderCopy)
				}
			}
		}
	}
	cloned.Sync()
	return cloned
}

func warmupParamValues(value interface{}) []interface{} {
	if value == nil {
		return nil
	}
	switch actual := value.(type) {
	case []interface{}:
		return actual
	}
	rType := reflect.TypeOf(value)
	if rType.Kind() == reflect.Slice {
		rValue := reflect.ValueOf(value)
		ret := make([]interface{}, rValue.Len())
		for i := 0; i < rValue.Len(); i++ {
			ret[i] = rValue.Index(i).Interface()
		}
		return ret
	}
	return []interface{}{value}
}

func (s *Service) topLevelWarmupMatcher(ctx context.Context, aView *view.View, statelet *view.Statelet, parent *expand.ViewContext) (*cache.ParmetrizedQuery, error) {
	if aView == nil || aView.Cache == nil || aView.Cache.Warmup == nil || statelet == nil || statelet.Template == nil {
		return nil, nil
	}
	indexColumn := strings.TrimSpace(aView.Cache.Warmup.IndexColumn)
	if indexColumn == "" {
		return nil, nil
	}
	matchParam := warmupIndexParameter(aView)
	if matchParam == nil {
		return nil, nil
	}
	liveSelector, selErr := statelet.Template.Selector(matchParam.Name)
	if selErr != nil || liveSelector == nil {
		return nil, selErr
	}
	value := liveSelector.Value(statelet.Template.Pointer())
	values := warmupParamValues(value)
	if len(values) == 0 {
		return nil, nil
	}
	matcher, err := s.warmupMatcher(ctx, aView, statelet, parent)
	if err != nil || matcher == nil {
		return matcher, err
	}
	matcher.By = indexColumn
	matcher.In = values
	return matcher, nil
}

func (s *Service) warmupMatcher(ctx context.Context, aView *view.View, statelet *view.Statelet, parent *expand.ViewContext) (*cache.ParmetrizedQuery, error) {
	if statelet == nil || statelet.Template == nil {
		return nil, nil
	}
	clonedTemplate := cloneStructologyState(statelet.Template)
	if clonedTemplate == nil {
		return nil, nil
	}
	if candidate := warmupIndexParameter(aView); candidate != nil {
		if clonedSelector, err := clonedTemplate.Selector(candidate.Name); err == nil && clonedSelector != nil {
			zero := reflect.Zero(clonedSelector.Type()).Interface()
			_ = clonedSelector.SetValue(clonedTemplate.Pointer(), zero)
		}
		if marker := clonedTemplate.Type().Marker(); marker != nil {
			clonedTemplate.EnsureMarker()
			if idx := marker.Index(candidate.Name); idx != -1 {
				_ = marker.Set(clonedTemplate.Pointer(), idx, false)
			}
		}
	}
	cloned := *statelet
	cloned.Template = clonedTemplate

	return s.sqlBuilder.CacheSQLWithOptions(ctx, aView, &cloned, nil, nil, parent)
}

func warmupIndexParameter(aView *view.View) *state.Parameter {
	if aView == nil || aView.Cache == nil || aView.Cache.Warmup == nil || aView.Template == nil {
		return nil
	}
	parameterName := strings.TrimSpace(aView.Cache.Warmup.IndexParameter)
	if parameterName == "" {
		return nil
	}
	for _, candidate := range aView.Template.Parameters {
		if candidate == nil {
			continue
		}
		if matchesWarmupParameter(candidate, parameterName) {
			return candidate
		}
	}
	return nil
}

func matchesWarmupParameter(candidate *state.Parameter, configured string) bool {
	if candidate == nil {
		return false
	}
	configured = strings.TrimSpace(configured)
	if configured == "" {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(candidate.Name), configured) {
		return true
	}
	if candidate.In != nil && strings.EqualFold(strings.TrimSpace(candidate.In.Name), configured) {
		return true
	}

	fieldName := warmupIndexFieldName(configured)
	if fieldName == "" {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(candidate.Name), fieldName) {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(candidate.Name), fieldName+"s") {
		return true
	}
	return false
}

func (s *Service) BuildCriteria(ctx context.Context, value interface{}, options *codec.CriteriaBuilderOptions) (*codec.Criteria, error) {
	baseView := view.Context(ctx)
	aSchema := state.NewSchema(reflect.TypeOf(value))
	resource := baseView.Resource()
	aType, err := state.NewType(state.WithSchema(aSchema), state.WithResource(resource))
	if err != nil {
		return nil, err
	}
	if err := aType.Init(state.WithResource(resource)); err != nil {
		return nil, err
	}
	aView, err := view.New("autogen", "", view.WithTemplate(view.NewTemplate(options.Expression, view.WithTemplateSchema(aSchema), view.WithTemplateParameters(aType.Parameters...))))
	if err != nil {
		return nil, err
	}
	err = aView.Template.Init(ctx, baseView.GetResource(), aView)
	if err != nil {
		return nil, err
	}
	stateType := structology.NewStateType(reflect.TypeOf(value))
	statelet := &view.Statelet{Template: stateType.NewState()}
	parametrizedSQL, err := s.sqlBuilder.Build(ctx, WithBuilderView(aView), WithBuilderStatelet(statelet))
	if err != nil {
		return nil, err
	}
	ret := &codec.Criteria{Expression: parametrizedSQL.SQL, Placeholders: parametrizedSQL.Args}
	return ret, nil
}

func (s *Service) queryInBatches(ctx context.Context, session *Session, aView *view.View, collector *view.Collector, visitor view.VisitorFn, info *response.SQLExecutions, batchData *view.BatchData, selector *view.Statelet) error {
	wg := &sync.WaitGroup{}
	db, err := aView.Db()
	if err != nil {
		return fmt.Errorf("failed to get db: %w", err)
	}

	var metaErr error
	if aView.Template.Summary != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var templateMeta *response.SQLExecution
			parentMeta := view.AsViewParam(aView, selector, batchData)
			templateMeta, metaErr = s.querySummary(ctx, session, aView, selector, batchData, collector, parentMeta)
			if templateMeta != nil {
				info.Append(templateMeta)
			}
		}()
	}

	executions, err := s.queryObjects(ctx, session, aView, selector, batchData, db, collector, visitor)
	if err != nil {
		return err
	}

	if len(executions) > 0 {
		info.Append(executions...)
	}
	wg.Wait()
	return metaErr
}

func (s *Service) queryObjects(ctx context.Context, session *Session, aView *view.View, selector *view.Statelet, batchData *view.BatchData, db *sql.DB, collector *view.Collector, visitor view.VisitorFn) ([]*response.SQLExecution, error) {

	if partitioned := aView.Partitioned; partitioned != nil {
		return s.queryWithPartitions(ctx, session, aView, selector, batchData, db, collector, visitor, partitioned)
	}
	readData := 0
	parametrizedSQL, columnInMatcher, err := s.buildParametrizedSQL(ctx, aView, selector, batchData, collector, session, nil)
	if err != nil {
		return nil, err
	}

	var parentProvider func(value interface{}) (interface{}, error)
	handler := func(row interface{}) error {
		row, err = aView.UnwrapDatabaseType(ctx, row)
		if err != nil {
			return err
		}
		readData++
		if fetcher, ok := row.(OnFetcher); ok {
			if aView.PublishParent && parentProvider == nil {
				parentProvider = collector.ParentRow(collector.Relation())
			}
			if ctx, err = s.getParentContext(ctx, row, collector, parentProvider); err != nil {
				return err
			}
			if err = fetcher.OnFetch(ctx); err != nil {
				return err
			}
		}
		return visitor(row)
	}
	execs, err := s.queryWithHandler(ctx, session, aView, collector, columnInMatcher, parametrizedSQL, db, handler, &readData)
	return execs, err
}

func (s *Service) getParentContext(ctx context.Context, row interface{}, collector *view.Collector, parentProvider func(value interface{}) (interface{}, error)) (context.Context, error) {
	if parentProvider == nil {
		return ctx, nil
	}
	parentRecord, err := parentProvider(row)
	if err != nil {
		return nil, fmt.Errorf("failed to get parentRecord row: %w", err)
	}
	if parentRecord == nil {
		return ctx, nil
	}
	parentKey := reflect.TypeOf(parentRecord)
	fetchContext := vcontext.WithValue(ctx, handler.DataSyncKey, collector.Parent().DataSync())
	fetchContext = vcontext.WithValue(fetchContext, parentKey, parentRecord)
	return fetchContext, nil
}

func (s *Service) queryWithHandler(ctx context.Context, session *Session, aView *view.View, collector *view.Collector, columnInMatcher *cache.ParmetrizedQuery, parametrizedSQL *cache.ParmetrizedQuery, db *sql.DB, handler func(row interface{}) error, readData *int) ([]*response.SQLExecution, error) {
	begin := time.Now()
	var cacheStats *cache.Stats

	var options = []read.Option{read.WithUnmappedFn(io.Resolve(collector.Resolve))}
	var err error
	if session.IsCacheEnabled(aView) {
		service, err := aView.Cache.Service()
		if err == nil {
			cacheStats = &cache.Stats{}
			options = append(options, read.WithCache(service), read.WithCacheStats(cacheStats))
		}
	}
	if columnInMatcher != nil {
		columnInMatcher.OnSkip = collector.OnSkip
		options = append(options, read.WithInMatcher(columnInMatcher))
	}
	if session.CacheRefresh {
		options = append(options, read.WithCacheRefresh(session.CacheRefresh))
	}

	stats, onDone := NewExecutionInfo(parametrizedSQL, cacheStats, collector)
	defer onDone()
	if session.DryRun {
		return []*response.SQLExecution{stats}, nil
	}

	retires := uint32(0)
BEGIN:
	reader, err := read.New(ctx, db, parametrizedSQL.SQL, collector.NewItem(), options...)

	isInvalidConnection := err != nil && strings.Contains(err.Error(), "invalid connection")
	if isInvalidConnection && atomic.AddUint32(&retires, 1) < 3 {
		db, err = aView.Connector.DB()
		if err != nil {
			return nil, fmt.Errorf("failed to connect to db: %w", err)
		}
		goto BEGIN
	}
	if err != nil {
		stats.SetError(err)
		anExec, err := s.HandleSQLError(err, session, aView, parametrizedSQL, stats)
		return []*response.SQLExecution{anExec}, err
	}

	defer func() {
		stmt := reader.Stmt()
		if stmt == nil {
			return
		}
		_ = stmt.Close()
	}()
	err = reader.QueryAll(ctx, handler, parametrizedSQL.Args...)

	isInvalidConnection = err != nil && strings.Contains(err.Error(), "invalid connection")
	if isInvalidConnection && atomic.AddUint32(&retires, 1) < 3 {
		db, err = aView.Connector.DB()
		if err != nil {
			return nil, fmt.Errorf("failed to connect to db: %w", err)
		}
		goto BEGIN
	}
	end := time.Now()

	aView.Logger.ReadingData(end.Sub(begin), parametrizedSQL.SQL, *readData, parametrizedSQL.Args, err)
	logCacheRead(ctx, aView, cacheStats, end.Sub(begin), *readData, parametrizedSQL.Args)
	if err != nil {
		stats.SetError(err)
		anExec, err := s.HandleSQLError(err, session, aView, parametrizedSQL, stats)
		return []*response.SQLExecution{anExec}, err
	}
	return []*response.SQLExecution{stats}, nil
}

func (s *Service) queryWithPartitions(ctx context.Context, session *Session, aView *view.View, selector *view.Statelet, batchData *view.BatchData, db *sql.DB, collector *view.Collector, visitor view.VisitorFn, partitioned *view.Partitioned) ([]*response.SQLExecution, error) {
	concurrency := aView.Partitioned.Concurrency
	if concurrency == 0 {
		concurrency = 2
	}
	partitioner := partitioned.Partitioner()
	wg := &sync.WaitGroup{}
	var err error
	partitions, err := partitioner.Partitions(ctx, db, aView)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition: %w", err)
	}

	var executions []*response.SQLExecution
	var mux sync.Mutex

	var parentProvider func(value interface{}) (interface{}, error)

	var rateLimit = make(chan bool, concurrency)
	var collectors = make([]*view.Collector, len(partitions))
	for i, partition := range partitions {
		wg.Add(1)
		rateLimit <- true
		go func(i int, partition *view.Partition) {
			defer func() {
				wg.Done()
				<-rateLimit
			}()
			collectors[i] = collector.Clone()
			parametrizedSQL, columnInMatcher, e := s.buildParametrizedSQL(ctx, aView, selector, batchData, collectors[i], session, partition)
			readData := 0
			handler := func(row interface{}) error {
				row, err = aView.UnwrapDatabaseType(ctx, row)
				if err != nil {
					return err
				}
				readData++
				if fetcher, ok := row.(OnFetcher); ok {
					if aView.PublishParent && parentProvider == nil {
						parentProvider = collector.ParentRow(collector.Relation())
					}

					if ctx, err = s.getParentContext(ctx, row, collector, parentProvider); err != nil {
						return err
					}
					if err = fetcher.OnFetch(ctx); err != nil {
						return err
					}
				}
				return nil
			}
			exec, e := s.queryWithHandler(ctx, session, aView, collectors[i], columnInMatcher, parametrizedSQL, db, handler, &readData)
			mux.Lock()
			if exec != nil {
				executions = append(executions, exec...)
			}
			mux.Unlock()
			if e != nil {
				err = e
			}
		}(i, partition)
		if err != nil {
			break
		}
	}
	wg.Wait()
	if len(collectors) == 0 || err != nil {
		return executions, err
	}

	result := collectors[0]

	for i := 1; i < len(collectors); i++ {
		second := collectors[i]
		if second != nil && second.Len() > 0 {
			merged := combineSlices(result.Dest(), second.Dest())
			result.SetDest(merged)
		}
	}

	if newReducer, ok := partitioner.(view.ReducerProvider); ok {
		reducer := newReducer.Reducer(ctx)
		reduced := reducer.Reduce(result.Dest())
		result.SetDest(reduced)
	}

	resultValue := reflect.ValueOf(result.Dest())
	for i := 0; i < resultValue.Len(); i++ {
		value := resultValue.Index(i).Interface()
		if err := visitor(value); err != nil {
			return executions, err
		}
	}

	collector.SetDest(result.Dest())

	return executions, err
}

func (s *Service) HandleSQLError(err error, session *Session, aView *view.View, matcher *cache.ParmetrizedQuery, stats *response.SQLExecution) (*response.SQLExecution, error) {
	aView.Logger.LogDatabaseErr(matcher.SQL, err, matcher.Args...)
	stats.Error = err.Error()
	return stats, fmt.Errorf("database error occured while fetching Data for view %v %w", aView.Name, err)
}

func logCacheRead(ctx context.Context, aView *view.View, stats *cache.Stats, elapsed time.Duration, rows int, args []interface{}) {
	if stats == nil {
		return
	}
	recordCacheReadMetrics(aView, stats)
	fmt.Printf("[INFO] datly cache read reqTraceId=%s view=%s source=%s type=%s found_warmup=%t found_lazy=%t records=%d rows=%d namespace=%s set=%s elapsed=%s args=%v\n",
		reqTraceID(ctx),
		aView.Name,
		cacheReadSource(stats),
		stats.Type,
		stats.FoundWarmup,
		stats.FoundLazy,
		stats.RecordsCounter,
		rows,
		stats.Namespace,
		stats.Dataset,
		elapsed,
		args)
}

func reqTraceID(ctx context.Context) string {
	if traceID := requesttrace.Current(ctx); traceID != "" {
		return traceID
	}
	return "unknown"
}

func recordCacheReadMetrics(aView *view.View, stats *cache.Stats) {
	if aView == nil || aView.Counter == nil || stats == nil {
		return
	}
	if stats.ErrorType != "" {
		aView.Counter.IncrementValue("cache:error")
		return
	}
	if stats.FoundWarmup {
		aView.Counter.IncrementValue("cache:hit")
		aView.Counter.IncrementValue("cache:warmup_hit")
		return
	}
	if stats.FoundLazy {
		aView.Counter.IncrementValue("cache:hit")
		aView.Counter.IncrementValue("cache:lazy_hit")
		return
	}
	if stats.Type == cache.TypeWrite {
		aView.Counter.IncrementValue("cache:miss")
		aView.Counter.IncrementValue("cache:miss_write")
		return
	}
	aView.Counter.IncrementValue("cache:miss")
}

func cacheReadSource(stats *cache.Stats) string {
	if stats.ErrorType != "" {
		return "error"
	}
	switch stats.Type {
	case cache.TypeReadMulti:
		return "warmup"
	case cache.TypeReadSingle:
		return "lazy"
	case cache.TypeWrite:
		return "miss_write"
	}
	if stats.FoundWarmup {
		return "warmup"
	}
	if stats.FoundLazy {
		return "lazy"
	}
	return "miss"
}

func NewExecutionInfo(index *cache.ParmetrizedQuery, cacheStats *cache.Stats, collector *view.Collector) (*response.SQLExecution, func()) {
	var cacheInfo *response.CacheStats
	if cacheStats != nil {
		cacheInfo = &response.CacheStats{}
	}
	var parentId string
	if parent := collector.Parent(); parent != nil {
		parentId = parent.Id
	}

	now := time.Now()
	ret := &response.SQLExecution{
		ID:         collector.Id,
		ParentID:   parentId,
		StartTime:  now,
		EndTime:    now,
		SQL:        index.SQL,
		Args:       index.Args,
		CacheStats: cacheInfo,
	}

	return ret, func() {
		now := time.Now()
		ret.EndTime = now
		ret.Rows = collector.Len()
		if cacheStats != nil && ret.CacheStats != nil {
			ret.CacheStats.Type = string(cacheStats.Type)
			ret.CacheStats.RecordsCounter = cacheStats.RecordsCounter
			ret.CacheStats.Key = cacheStats.Key
			ret.CacheStats.Dataset = cacheStats.Dataset
			ret.CacheStats.Namespace = cacheStats.Namespace
			ret.CacheStats.FoundWarmup = cacheStats.FoundWarmup
			ret.CacheStats.FoundLazy = cacheStats.FoundLazy
			ret.CacheStats.ErrorType = cacheStats.ErrorType
			ret.CacheStats.ErrorCode = int(cacheStats.ErrorCode)
			ret.CacheStats.ExpiryTime = cacheStats.ExpiryTime
		}
	}
}

// New creates Service instance
func New() *Service {
	ret := &Service{
		sqlBuilder: NewBuilder(),
	}
	return ret
}
