package reader

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	rsql "github.com/viant/datly/sql/builder"
	rcollector "github.com/viant/datly/sql/reader/collector"
	"github.com/viant/sqlx/io/read/cache"
	xreader "github.com/viant/xdatly/reader"
)

type partitionSet struct {
	items       []xreader.Partition
	concurrency int
}

type partitionQuery struct {
	query   *cache.ParmetrizedQuery
	options []rsql.BuilderOption
}

// partitionSource exposes enumeration only while a relation collects every
// configured/adaptive batch. Reduction runs once after that complete fetch.
type partitionSource struct{ xreader.Partitioner }

type partitionReduction struct {
	plan     *ViewPlan
	provider xreader.ReducerProvider
}

func (p partitionReduction) reduce(ctx context.Context, rows any) (any, error) {
	reducer := p.provider.Reducer(ctx)
	if reducer == nil {
		return nil, fmt.Errorf("partition reducer is nil for view %s", viewName(p.plan.View))
	}
	reduced, err := reducer.Reduce(ctx, rows)
	if err != nil {
		return nil, fmt.Errorf("reduce partitions for view %s: %w", viewName(p.plan.View), err)
	}
	return reduced, nil
}

func (s *Service) resolvePartitions(ctx context.Context, plan *ViewPlan, db *sql.DB) (*partitionSet, error) {
	if plan == nil || plan.View == nil {
		return nil, fmt.Errorf("partitioned reader view plan is required")
	}
	view := plan.View
	if plan.Partitioner == nil {
		return nil, fmt.Errorf("partitioner is not prepared for view %s", viewName(view))
	}
	config := view.Spec.Partitioning
	if config == nil {
		return nil, fmt.Errorf("partition metadata is not prepared for view %s", viewName(view))
	}
	partitions, err := plan.Partitioner.Partitions(ctx, xreader.PartitionRequest{
		DB: db, View: viewName(view), Arguments: append([]string(nil), config.Arguments...),
	})
	if err != nil {
		return nil, fmt.Errorf("resolve partitions for view %s: %w", viewName(view), err)
	}
	concurrency := config.Concurrency
	if concurrency <= 0 {
		concurrency = 2
	}
	if concurrency > len(partitions) {
		concurrency = len(partitions)
	}
	return &partitionSet{items: partitions, concurrency: concurrency}, nil
}

func (p *partitionSet) run(ctx context.Context, worker func(context.Context, int, xreader.Partition) error) error {
	if p == nil {
		return ctx.Err()
	}
	return (fetchSet{count: len(p.items), concurrency: p.concurrency}).run(ctx, func(ctx context.Context, index int) error { return worker(ctx, index, p.items[index]) })
}

type partitionRead struct {
	service     *Service
	session     *Session
	plan        *ViewPlan
	db          *sql.DB
	options     []rsql.BuilderOption
	collector   *rcollector.Collector
	parentCount int
	read        *viewRead
}

func (p partitionRead) run(ctx context.Context) error {
	s, plan, db, baseOptions, canonical := p.service, p.plan, p.db, p.options, p.collector
	partitions, err := s.resolvePartitions(ctx, plan, db)
	if err != nil {
		return err
	}
	if len(partitions.items) == 0 {
		return nil
	}
	connection, err := viewConnection(ctx, p.session, plan)
	if err != nil {
		return err
	}
	prepared := make([]partitionQuery, len(partitions.items))
	// Preflight every partition before any row hooks run. An oversized
	// relation batch can then be split without scanning a partition twice.
	for index, partition := range partitions.items {
		if err := ctx.Err(); err != nil {
			return err
		}
		options := append([]rsql.BuilderOption(nil), baseOptions...)
		options = append(options, rsql.WithBuilderPartition(&rsql.PartitionInput{Table: partition.Table, Expression: partition.Expression, Args: append([]any(nil), partition.Args...)}))
		query, err := rsql.NewBuilder().Build(ctx, options...)
		if err != nil {
			return err
		}
		if err := (parameterBudget{dialect: connection.Dialect}).check(len(query.Args)); err != nil {
			return err
		}
		prepared[index] = partitionQuery{query: query, options: options}
	}
	ordered := make([]*rcollector.Collector, len(partitions.items))
	if err := partitions.run(ctx, func(workCtx context.Context, index int, partition xreader.Partition) error {
		collector := canonical.PartitionCopy()
		if err := p.readOne(workCtx, prepared[index], collector); err != nil {
			return err
		}
		ordered[index] = collector
		return nil
	}); err != nil {
		return err
	}

	if provider, ok := plan.Partitioner.(xreader.ReducerProvider); ok {
		compiledView := plan.Collector
		if compiledView == nil || compiledView.Schema.SliceType() == nil {
			return fmt.Errorf("partition collector view %s is not compiled", viewName(plan.View))
		}
		rows := reflect.MakeSlice(compiledView.Schema.SliceType(), 0, 0)
		for _, collector := range ordered {
			rows = reflect.AppendSlice(rows, reflect.ValueOf(collector.Dest()))
		}
		reduced, err := (partitionReduction{plan: plan, provider: provider}).reduce(ctx, rows.Interface())
		if err != nil {
			return err
		}
		return canonical.AppendSlice(ctx, reduced)
	}
	for _, collector := range ordered {
		if err := canonical.AppendPartition(ctx, collector); err != nil {
			return err
		}
	}
	return nil
}

func (p partitionRead) readOne(ctx context.Context, prepared partitionQuery, collector *rcollector.Collector) error {
	session, plan, db, parentCount := p.session, p.plan, p.db, p.parentCount
	view := plan.View
	options, query := prepared.options, prepared.query
	var matchers []*cache.ParmetrizedQuery
	if collector.Relation() != nil {
		matcher, err := rsql.NewBuilder().QueryMatcher(ctx, query, options...)
		if err != nil {
			return err
		}
		if !collector.Relation().Of.MatchStrategy.ReadAll() && parentCount <= 1 {
			matcher.Offset = 0
			matcher.Limit = 0
		}
		matcher.OnSkip = collector.OnSkip
		matchers = append(matchers, matcher)
	}
	scan := newReaderOptions(session, view).rows(collector.NewItem(), unmappedResolver(collector), matchers...)
	visitor := newRowHookVisitor(ctx, view, collector, collector.Visitor(ctx))
	visitor.decoder = scan.decoder
	visitor.evidence = scan.evidence
	parent := ""
	if owner := collector.Parent(); owner != nil {
		parent = owner.Id
	}
	return scan.query(ctx, rowQuery{collector: collector, db: db, query: query, visit: visitor.Visit, read: p.read, id: collector.Id, parent: parent})
}
