package reader

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	rsql "github.com/viant/datly/sql/builder"
	rcollector "github.com/viant/datly/sql/reader/collector"
	"github.com/viant/sqlx/io/read/cache"
	xhandler "github.com/viant/xdatly/handler"
	xreader "github.com/viant/xdatly/reader"
	xstate "github.com/viant/xdatly/state"
)

type relationRead struct {
	read        *viewRead
	service     *Service
	ctx         context.Context
	session     *Session
	input       reflect.Value
	binder      xhandler.Binder
	selector    invocationSelectors
	child       *rcollector.Collector
	plan        *ViewPlan
	connection  dsql.Connection
	parentCount int
	readAll     bool
}

type relationBatch struct {
	placeholders []interface{}
	composite    [][]interface{}
}

func (r *relationRead) Read(placeholders []interface{}, composite [][]interface{}, columns []string) (err error) {
	if r.read == nil {
		r.session.initMetrics(r.service.recorder)
		r.read = r.session.beginView(r.ctx, r.plan.View)
		r.read.collector = r.child
		defer r.read.finish(&err)
	}
	count := len(placeholders)
	if len(composite) > 0 {
		count = len(composite)
	}
	r.parentCount = count
	r.readAll = r.child.ReadAll()
	batches := r.batches(placeholders, composite)
	if provider, ok := r.plan.Partitioner.(xreader.ReducerProvider); ok {
		worker := *r
		plan := *r.plan
		plan.Partitioner = partitionSource{Partitioner: r.plan.Partitioner}
		worker.plan = &plan
		worker.child = r.child.PartitionCopy()
		if err := worker.readBatches(batches, columns); err != nil {
			return err
		}
		if err := r.ctx.Err(); err != nil {
			return err
		}
		rows, err := (partitionReduction{plan: r.plan, provider: provider}).reduce(r.ctx, worker.child.Dest())
		if err != nil {
			return err
		}
		return r.child.AppendSlice(r.ctx, rows)
	}
	return r.readBatches(batches, columns)
}

// readBatches retains the canonical match mode even when a worker's isolated
// collector deliberately postpones attachment to its parent.
func (r *relationRead) readBatches(batches []relationBatch, columns []string) error {
	if len(batches) <= 1 {
		return (fetchSet{count: len(batches), concurrency: 1}).run(r.ctx, func(ctx context.Context, index int) error {
			batch := batches[index]
			err := r.readBatch(batch.placeholders, batch.composite, columns)
			var limit *parameterLimitError
			if !errors.As(err, &limit) {
				return err
			}
			worker := *r
			worker.child = r.child.PartitionCopy()
			if err := worker.readWithinBudget(batch, columns); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return r.child.AppendPartition(ctx, worker.child)
		})
	}
	ordered := make([]*rcollector.Collector, len(batches))
	if err := (fetchSet{count: len(batches), concurrency: r.plan.View.Spec.BatchConcurrency}).run(r.ctx, func(ctx context.Context, index int) error {
		batch := batches[index]
		worker := *r
		worker.ctx = ctx
		worker.child = r.child.PartitionCopy()
		if err := worker.readWithinBudget(batch, columns); err != nil {
			return err
		}
		ordered[index] = worker.child
		return nil
	}); err != nil {
		return err
	}
	if err := r.ctx.Err(); err != nil {
		return err
	}
	for _, batch := range ordered {
		if err := r.child.AppendPartition(r.ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (r *relationRead) batches(placeholders []interface{}, composite [][]interface{}) []relationBatch {
	count := len(placeholders)
	if len(composite) > 0 {
		count = len(composite)
	}
	batchSize := r.plan.View.Spec.BatchSize
	if batchSize <= 0 || count <= batchSize {
		return []relationBatch{{placeholders: placeholders, composite: composite}}
	}
	result := make([]relationBatch, 0, (count+batchSize-1)/batchSize)
	for start := 0; start < count; start += batchSize {
		end := start + batchSize
		if end > count {
			end = count
		}
		if len(composite) > 0 {
			result = append(result, relationBatch{composite: composite[start:end]})
			continue
		}
		result = append(result, relationBatch{placeholders: placeholders[start:end]})
	}
	return result
}

func (r *relationRead) readBatch(placeholders []interface{}, composite [][]interface{}, columns []string) error {
	view := r.plan.View
	options := []rsql.BuilderOption{
		rsql.WithBuilderView(view),
		rsql.WithBuilderCriteriaCompiler(r.plan.Criteria),
		rsql.WithBuilderSelector(r.selector.forView(view)),
		rsql.WithBuilderProjection(relationProjection(view, r.selector.forView(view), r.child.Relation())),
		rsql.WithBuilderInput(r.input.Elem()),
		rsql.WithBuilderParameterResolver(r.session.Parameters),
		rsql.WithBuilderRelation(r.child.Relation()),
		rsql.WithBuilderTemplate(r.plan.Template),
		rsql.WithBuilderBinder(r.binder),
		rsql.WithBuilderDialect(r.connection.Dialect),
		rsql.WithBuilderSkipRelationFilter(r.readAll),
	}
	if r.readAll || r.parentCount > 1 {
		options = append(options, rsql.WithBuilderExcludePagination(true))
	}
	if len(composite) > 0 {
		options = append(options, rsql.WithBuilderCompositeArgs(columns, composite))
	} else {
		options = append(options, rsql.WithBuilderPositionalArgs(toAnySlice(placeholders)))
	}
	if r.plan.Partitioner != nil {
		return (partitionRead{service: r.service, session: r.session, plan: r.plan, db: r.connection.DB, options: options, collector: r.child, parentCount: r.parentCount, read: r.read}).run(r.ctx)
	}
	query, err := rsql.NewBuilder().Build(r.ctx, options...)
	if err != nil {
		return err
	}
	if err := (parameterBudget{dialect: r.connection.Dialect}).check(len(query.Args)); err != nil {
		return err
	}
	matcher, err := rsql.NewBuilder().QueryMatcher(r.ctx, query, options...)
	if err != nil {
		return fmt.Errorf("build relation cache matcher for %s: %w", view.Spec.Name, err)
	}
	if err := r.applyWarmupMatcher(r.ctx, query, matcher, columns); err != nil {
		return fmt.Errorf("build relation warmup matcher for %s: %w", view.Spec.Name, err)
	}
	if !r.readAll && r.parentCount <= 1 {
		matcher.Offset = 0
		matcher.Limit = 0
	}
	matcher.OnSkip = r.child.OnSkip
	scan := newReaderOptions(r.session, view).rows(r.child.NewItem(), unmappedResolver(r.child), matcher)
	visitor := newRowHookVisitor(r.ctx, view, r.child, r.child.Visitor(r.ctx))
	visitor.decoder = scan.decoder
	visitor.evidence = scan.evidence
	parent := ""
	if p := r.child.Parent(); p != nil {
		parent = p.Id
	}
	return scan.query(r.ctx, rowQuery{collector: r.child, db: r.connection.DB, query: query, visit: visitor.Visit, read: r.read, id: r.child.Id, parent: parent})
}

func (r *relationRead) applyWarmupMatcher(ctx context.Context, query, matcher *cache.ParmetrizedQuery, columns []string) error {
	if r == nil || r.session == nil || r.plan == nil || matcher == nil {
		return nil
	}
	view := r.plan.View
	if r.session.ReadCaches[view] == nil || view.Cache == nil {
		return nil
	}
	warmups, err := view.Cache.EffectiveWarmups()
	if err != nil {
		return err
	}
	settings := selectRelationWarmup(warmups, columns)
	if settings == nil {
		return nil
	}
	selector := r.selector.forView(view).Clone()
	projection := viewProjection(view, selector)
	if selector == nil {
		selector = &xstate.Selector{}
	}
	selector.Fields = append([]string(nil), settings.FieldNames...)
	selector.Columns = nil
	identity, err := rsql.NewBuilder().CacheSQL(ctx,
		rsql.WithBuilderComponent(r.session.Component), rsql.WithBuilderView(view), rsql.WithBuilderCriteriaCompiler(r.plan.Criteria),
		rsql.WithBuilderSelector(selector), rsql.WithBuilderProjection(viewProjection(view, selector)),
		rsql.WithBuilderInput(r.input.Elem()), rsql.WithBuilderParameterResolver(r.session.Parameters),
		rsql.WithBuilderTemplate(r.plan.Template), rsql.WithBuilderBinder(r.binder), rsql.WithBuilderDialect(r.connection.Dialect))
	if err != nil {
		return err
	}
	stored, requested := r.plan.cacheProjection(settings.FieldNames), r.plan.cacheProjection(projection)
	if view.IsGroupable() {
		stored, err = (dsql.CacheProjection{SQL: identity.SQL, View: view}).Fields()
		if err != nil {
			return nil
		}
		requested, err = (dsql.CacheProjection{SQL: query.SQL, View: view}).Fields()
		if err != nil {
			return nil
		}
	}
	if _, compatible, _, err := (cache.Projection{Stored: stored}).Indexes(requested); err != nil {
		return err
	} else if !compatible {
		return nil
	}
	if indexColumn := strings.TrimSpace(settings.IndexColumn); indexColumn != "" {
		matcher.By = indexColumn
	}
	matcher.IdentitySQL = identity.SQL
	matcher.IdentityArgs = identity.Args
	matcher.Limit = identity.Limit
	matcher.Offset = identity.Offset
	for _, field := range requested {
		field.DimensionKey, field.MeasureKey = "", ""
		matcher.RequestedFields = append(matcher.RequestedFields, field)
	}
	return nil
}

// selectRelationWarmup picks the effective warmup whose index column matches the
// relation link columns; explicit priority wins and declaration order (singular
// first) breaks ties. Without an indexed match, a non-indexed warmup keeps the
// existing singular identity behavior. A single declared warmup is used as-is.
func selectRelationWarmup(warmups []*spec.CacheWarmupSettings, columns []string) *spec.CacheWarmupSettings {
	if len(warmups) == 1 {
		return warmups[0]
	}
	var indexed, fallback *spec.CacheWarmupSettings
	for _, candidate := range warmups {
		if candidate == nil {
			continue
		}
		indexColumn := strings.TrimSpace(candidate.IndexColumn)
		if indexColumn == "" {
			if fallback == nil || candidate.Priority >= fallback.Priority {
				fallback = candidate
			}
			continue
		}
		if !matchesRelationIndexColumn(indexColumn, columns) {
			continue
		}
		if indexed == nil || candidate.Priority > indexed.Priority {
			indexed = candidate
		}
	}
	if indexed != nil {
		return indexed
	}
	return fallback
}

func matchesRelationIndexColumn(indexColumn string, columns []string) bool {
	for _, column := range columns {
		if strings.EqualFold(normalizeWarmupColumnName(column), normalizeWarmupColumnName(indexColumn)) {
			return true
		}
	}
	return false
}

func normalizeWarmupColumnName(input string) string {
	input = strings.TrimSpace(input)
	if index := strings.LastIndex(input, "."); index != -1 {
		input = input[index+1:]
	}
	return strings.TrimSpace(input)
}
