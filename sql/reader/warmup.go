package reader

import (
	"context"
	"fmt"
	"reflect"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/builder"
	"github.com/viant/sqlx/io/read/cache"
	xhandler "github.com/viant/xdatly/handler"
	xreader "github.com/viant/xdatly/reader"
	xstate "github.com/viant/xdatly/state"
)

// Warmup prepares queries through the same reader owners as normal reads and
// populates the exact native SQLX cache assigned to each prepared view.
func (e *Execution) Warmup(ctx context.Context, invocation dexec.ReaderWarmupInvocation) (int, error) {
	if e == nil {
		return 0, fmt.Errorf("reader execution is required")
	}
	session := e.session()
	session.Parameters = invocation.Parameters
	if err := session.Init(); err != nil {
		return 0, err
	}
	session.initMetrics(e.service.recorder)
	view, err := session.Artifact.ViewIndex.Resolve(invocation.Request.View)
	if err != nil {
		return 0, err
	}
	plan := session.Artifact.ViewPlanFor(view)
	if plan == nil {
		return 0, fmt.Errorf("view %q has no reader plan", view.Spec.Name)
	}
	input := reflect.ValueOf(invocation.Input)
	if !input.IsValid() || input.Kind() != reflect.Pointer || input.IsNil() || input.Elem().Type() != session.InputType {
		return 0, fmt.Errorf("warmup input must be *%s", session.InputType)
	}
	// An explicit request executes exactly one warmup policy. Without one, every
	// effective view warmup runs in declaration order: singular first, then plural.
	var effective []*spec.CacheWarmupSettings
	if invocation.Request.Settings != nil {
		effective = []*spec.CacheWarmupSettings{invocation.Request.Settings}
	} else if view.Cache != nil {
		effective, err = view.Cache.EffectiveWarmups()
		if err != nil {
			return 0, err
		}
	}
	if len(effective) == 0 {
		return 0, fmt.Errorf("view %q has no warmup settings", view.Spec.Name)
	}
	selectors, err := resolveInvocationSelectors(ctx, session, input, invocation.Binder)
	if err != nil {
		return 0, err
	}
	warmup := warmupExecution{owner: e, session: session, input: input, binder: invocation.Binder, selectors: selectors}
	total := 0
	for _, settings := range effective {
		count, err := e.warmupWithSettings(ctx, session, warmup, plan, settings)
		total += count
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func (e *Execution) warmupWithSettings(ctx context.Context, session *Session, warmup warmupExecution, plan *ViewPlan, settings *spec.CacheWarmupSettings) (int, error) {
	if settings.Limit != nil && *settings.Limit < 0 || settings.MaxCases != nil && *settings.MaxCases < 0 {
		return 0, fmt.Errorf("warmup limits must be non-negative")
	}
	selected := []*ViewPlan{plan}
	if settings.IndexMeta {
		for _, relation := range plan.Relations {
			if relation.Relation.IsOutput() {
				selected = append(selected, relation.Target)
			}
		}
	}
	for _, candidate := range selected {
		if session.ReadCaches[candidate.View] == nil {
			return 0, fmt.Errorf("view %q has no native read cache", candidate.View.Spec.Name)
		}
	}
	total, err := warmup.run(ctx, plan, nil, settings)
	if err != nil || !settings.IndexMeta {
		return total, err
	}
	for _, relation := range plan.Relations {
		if !relation.Relation.IsOutput() {
			continue
		}
		policies, err := relation.Target.View.Cache.EffectiveWarmups()
		if err != nil {
			return total, err
		}
		if len(policies) == 0 {
			policy := settings.Clone()
			policy.IndexMeta = false
			policy.IndexColumn = ""
			policy.FieldNames = nil
			policies = []*spec.CacheWarmupSettings{policy}
		}
		for _, policy := range policies {
			count, err := warmup.run(ctx, relation.Target, relation, policy)
			total += count
			if err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

type warmupExecution struct {
	owner     *Execution
	session   *Session
	input     reflect.Value
	binder    xhandler.Binder
	selectors invocationSelectors
}

func (w *warmupExecution) run(ctx context.Context, plan *ViewPlan, derived *RelationPlan, settings *spec.CacheWarmupSettings) (int, error) {
	if w.session.SQL != nil && w.session.SQL.Tx != nil {
		return 0, fmt.Errorf("transactional reader does not support cache warmup")
	}
	view := plan.View
	connector := plan.Connector
	if settings.Connector != "" {
		connector = settings.Connector
	}
	connection, err := w.session.SQL.Resolve(ctx, connector)
	if err != nil {
		return 0, err
	}
	selector := w.selectors.forView(view).Clone()
	if len(settings.FieldNames) > 0 {
		if selector == nil {
			selector = &xstate.Selector{}
		}
		selector.Fields = append([]string(nil), settings.FieldNames...)
	}
	var output *outputRelationExecution
	if derived != nil {
		root, err := w.owner.service.nonWindowQuery(ctx, w.session, w.input, w.binder, w.selectors)
		if err != nil {
			return 0, err
		}
		selectors := invocationSelectors{}
		for view, value := range w.selectors {
			selectors[view] = value
		}
		selectors[view] = selector
		output = &outputRelationExecution{service: w.owner.service, session: w.session, input: w.input, binder: w.binder, selectors: selectors, planned: derived, root: root, db: connection.DB}
	}
	warm := func(ctx context.Context, partition *xreader.Partition) (_ int, err error) {
		observation := w.session.beginView(ctx, view)
		w.session.recorder.Pending(w.session.pendingScope, 1)
		defer w.session.recorder.Pending(w.session.pendingScope, -1)
		observation.metric.Type = "WARMUP"
		defer observation.finish(&err)
		var query *cache.ParmetrizedQuery
		if output != nil {
			query, err = output.query(ctx, partition)
		} else {
			query, err = builder.NewBuilder().CacheSQL(ctx,
				builder.WithBuilderComponent(w.session.Component), builder.WithBuilderView(view), builder.WithBuilderCriteriaCompiler(plan.Criteria),
				builder.WithBuilderSelector(selector), builder.WithBuilderProjection(viewProjection(view, selector)),
				builder.WithBuilderInput(w.input.Elem()), builder.WithBuilderParameterResolver(w.session.Parameters),
				builder.WithBuilderTemplate(plan.Template), builder.WithBuilderBinder(w.binder), builder.WithBuilderDialect(connection.Dialect))
			if err == nil && partition != nil {
				query, err = builder.NewBuilder().ShapeBound(query, builder.WithBuilderView(view), builder.WithBuilderExcludePagination(true), builder.WithBuilderPartition(&builder.PartitionInput{Table: partition.Table, Expression: partition.Expression, Args: partition.Args}))
			}
		}
		if err != nil {
			return 0, err
		}
		if settings.Limit != nil && *settings.Limit > 0 {
			limited, err := builder.NewBuilder().ShapeBound(query, builder.WithBuilderControls(&spec.ViewControls{Limit: settings.Limit}), builder.WithBuilderDialect(connection.Dialect))
			if err != nil {
				return 0, err
			}
			if settings.IndexColumn != "" {
				limited.IdentitySQL = query.SQL
				limited.IdentityArgs = query.Args
			}
			query = limited
		}
		execution := observation.execution(query, "", "")
		defer func() { observation.completeSQL(execution, nil, -1, err) }()
		ctx = cache.WithCreationObserver(ctx, func(kind string, entries int) { w.session.recorder.Created(observation.scope, kind, entries) })
		return w.session.ReadCaches[view].IndexBy(ctx, connection.DB, settings.IndexColumn, query.SQL, query.Args, query)
	}
	if plan.Partitioner == nil {
		return warm(ctx, nil)
	}
	partitions, err := w.owner.service.resolvePartitions(ctx, plan, connection.DB)
	if err != nil {
		return 0, err
	}
	counts := make([]int, len(partitions.items))
	err = partitions.run(ctx, func(ctx context.Context, index int, partition xreader.Partition) error {
		count, err := warm(ctx, &partition)
		counts[index] = count
		return err
	})
	total := 0
	for _, count := range counts {
		total += count
	}
	return total, err
}
