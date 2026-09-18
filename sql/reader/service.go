package reader

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"unsafe"

	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/observability"
	dsql "github.com/viant/datly/sql"
	rsql "github.com/viant/datly/sql/builder"
	rcollector "github.com/viant/datly/sql/reader/collector"
	sqlxio "github.com/viant/sqlx/io"
	"github.com/viant/sqlx/io/read/cache"
	xhandler "github.com/viant/xdatly/handler"
	xstate "github.com/viant/xdatly/state"
)

// Service is the rebuilt reader capability. It restores a reader-owned
// execution center rather than pushing read behavior back into the root runtime
// facade.
type Service struct {
	relationFetchConcurrency int
	recorder                 *observability.Recorder
}

type Option func(*Service)

func WithRelationFetchConcurrency(concurrency int) Option {
	return func(service *Service) {
		service.relationFetchConcurrency = concurrency
	}
}

func NewService(options ...Option) *Service {
	service := &Service{relationFetchConcurrency: defaultRelationFetchConcurrency, recorder: observability.NewRecorder(nil)}
	for _, option := range options {
		if option != nil {
			option(service)
		}
	}
	return service
}

// Read executes a compiled reader request with input already prepared by the
// unified handler engine. Binding and transport concerns stay outside SQL.
func (s *Service) Read(ctx context.Context, session *Session, input any, binder xhandler.Binder) (_ any, err error) {
	if session != nil {
		session.Projection = nil
		session.outputSlots = nil
	}
	if err := session.Init(); err != nil {
		return nil, err
	}
	session.initMetrics(s.recorder)
	session.recorder.Pending(session.pendingScope, 1)
	defer session.recorder.Pending(session.pendingScope, -1)
	session.rootRead = session.beginView(ctx, session.Artifact.Root.View)
	defer session.rootRead.finish(&err)
	value := reflect.ValueOf(input)
	if !value.IsValid() || value.Kind() != reflect.Ptr || value.IsNil() || value.Elem().Type() != session.InputType {
		return nil, fmt.Errorf("reader bound input must be *%s, got %T", session.InputType, input)
	}
	selectors, err := resolveInvocationSelectors(ctx, session, value, binder)
	if err != nil {
		return nil, err
	}
	actual, err := s.readBound(ctx, session, value, binder, selectors)
	if err == nil {
		err = session.outputAccessors.writeMetrics(actual, session.Metrics)
	}
	if err == nil && !session.DryRun {
		err = session.outputAccessors.writeSuccess(actual)
	}
	if err == nil && !session.DryRun && dexec.WantsOutputSelection(ctx) {
		filter, selectionErr := session.selectedOutput(selectors)
		if selectionErr != nil {
			return nil, selectionErr
		}
		dexec.PublishOutputSelection(ctx, actual, filter)
	}
	return actual, err
}

func (s *Service) readBound(ctx context.Context, session *Session, input reflect.Value, binder xhandler.Binder, selectors invocationSelectors) (_ any, err error) {
	root := session.Artifact.Root
	read := session.rootRead
	rootSelector := selectors.forView(root.View)
	rootConnection, err := viewConnection(ctx, session, root)
	if err != nil {
		return nil, err
	}
	rootBuilderOptions := []rsql.BuilderOption{
		rsql.WithBuilderComponent(session.Component),
		rsql.WithBuilderView(root.View),
		rsql.WithBuilderCriteriaCompiler(root.Criteria),
		rsql.WithBuilderSelector(rootSelector),
		rsql.WithBuilderProjection(viewProjection(root.View, rootSelector)),
		rsql.WithBuilderInput(input.Elem()),
		rsql.WithBuilderParameterResolver(session.Parameters),
		rsql.WithBuilderTemplate(root.Template),
		rsql.WithBuilderBinder(binder),
		rsql.WithBuilderDialect(rootConnection.Dialect),
	}
	query, err := rsql.NewBuilder().Build(ctx, rootBuilderOptions...)
	if err != nil {
		return nil, err
	}
	if session.DryRun {
		if root.Partitioner != nil {
			return nil, fmt.Errorf("reader dry-run does not support partitioned roots")
		}
		return &dexec.ReadPlan{SQL: query.SQL, Args: append([]any(nil), query.Args...)}, nil
	}
	if err := (rootCacheMatcher{session: session, input: input, binder: binder, selectors: selectors, query: query}).apply(ctx); err != nil {
		return nil, err
	}
	if session.OutputType == nil {
		return nil, nil
	}
	outputType := session.OutputType
	if !session.Artifact.DirectOutput && outputType.Kind() == reflect.Ptr {
		outputType = outputType.Elem()
	}
	actual, rootCollector, rootField, rootDest, err := s.readRoot(ctx, session, rootConnection.DB, query, rootBuilderOptions, session.Artifact.OutputViewField, outputType, session.Artifact.DirectOutput)
	if err != nil {
		return nil, err
	}
	rows := 0
	if rootCollector != nil {
		rows = rootCollector.Len()
	}
	read.done(rows, nil)
	relationScheduler := newRelationScheduler(ctx, s.relationConcurrency())
	defer relationScheduler.Close()
	if err := s.bindRelationsScheduled(relationScheduler, session, input, binder, selectors, rootCollector); err != nil {
		return nil, err
	}
	if !session.Artifact.DirectOutput {
		if err := s.bindOutputRelations(ctx, session, input, binder, selectors, actual, root.Relations); err != nil {
			return nil, err
		}
	}
	if rootCollector != nil {
		if err := rootCollector.MergeData(); err != nil {
			return nil, err
		}
		if err := rootCollector.AssembleTrees(); err != nil {
			return nil, err
		}
		if err := rootCollector.RunOnRelation(ctx); err != nil {
			return nil, err
		}
		if session.Artifact.DirectOutput {
			converted, err := convertCollectorResult(rootDest.Elem(), session.OutputType)
			if err != nil {
				return nil, err
			}
			actual = converted.Interface()
		} else if rootField.IsValid() && rootField.CanSet() {
			converted, err := convertCollectorResult(rootDest.Elem(), rootField.Type())
			if err != nil {
				return nil, err
			}
			rootField.Set(converted)
		}
	}
	session.Data = actual
	if err = session.completeProjection(rootCollector); err != nil {
		return nil, err
	}
	return actual, nil
}

func (s *Service) readRoot(ctx context.Context, session *Session, db *sql.DB, query *cache.ParmetrizedQuery, builderOptions []rsql.BuilderOption, viewField string, outputType reflect.Type, direct bool) (any, *rcollector.Collector, reflect.Value, reflect.Value, error) {
	var output reflect.Value
	if !direct {
		output = reflect.New(outputType)
	}
	if viewField == "" && !direct {
		return output.Interface(), nil, reflect.Value{}, reflect.Value{}, nil
	}
	rootField := reflect.Value{}
	if !direct {
		rootField = output.Elem().FieldByName(viewField)
		if !rootField.IsValid() || !rootField.CanAddr() {
			return nil, nil, reflect.Value{}, reflect.Value{}, nil
		}
	}
	root := session.Artifact.Root
	view := root.View
	compiledView := root.Collector
	if compiledView == nil || compiledView.Schema.SliceType() == nil {
		return nil, nil, reflect.Value{}, reflect.Value{}, fmt.Errorf("root collector view is not compiled")
	}
	rootDest := reflect.New(compiledView.Schema.SliceType())
	rootDest.Elem().Set(reflect.MakeSlice(compiledView.Schema.SliceType(), 0, 0))
	rootCollector := rcollector.NewCollector(compiledView, rootDest.Interface(), false)
	session.rootRead.collector = rootCollector
	if session.CollectProjection {
		if err := rootCollector.EnableProvenance(); err != nil {
			return nil, nil, reflect.Value{}, reflect.Value{}, err
		}
	}
	if root.Partitioner != nil {
		if err := (partitionRead{service: s, session: session, plan: root, db: db, options: builderOptions, collector: rootCollector, read: session.rootRead}).run(ctx); err != nil {
			return nil, nil, reflect.Value{}, reflect.Value{}, err
		}
		rootCollector.Fetched()
		return reflectOutput(output), rootCollector, rootField, rootDest, nil
	}
	scan := newReaderOptions(session, view).rows(rootCollector.NewItem(), unmappedResolver(rootCollector), query)
	visitor := newRowHookVisitor(ctx, view, rootCollector, rootCollector.Visitor(ctx))
	visitor.decoder = scan.decoder
	visitor.evidence = scan.evidence
	if err := scan.query(ctx, rowQuery{collector: rootCollector, db: db, query: query, visit: visitor.Visit, read: session.rootRead, id: rootCollector.Id}); err != nil {
		return nil, nil, reflect.Value{}, reflect.Value{}, err
	}
	rootCollector.Fetched()
	return reflectOutput(output), rootCollector, rootField, rootDest, nil
}

func reflectOutput(output reflect.Value) any {
	if !output.IsValid() {
		return nil
	}
	return output.Interface()
}

func (s *Service) bindRelations(ctx context.Context, session *Session, input reflect.Value, binder xhandler.Binder, selectors invocationSelectors, collector *rcollector.Collector) error {
	scheduler := newRelationScheduler(ctx, s.relationConcurrency())
	defer scheduler.Close()
	return s.bindRelationsScheduled(scheduler, session, input, binder, selectors, collector)
}

func (s *Service) bindRelationsScheduled(scheduler *relationScheduler, session *Session, input reflect.Value, binder xhandler.Binder, selectors invocationSelectors, collector *rcollector.Collector) error {
	if collector == nil {
		return nil
	}
	children := collector.Relations(selectorStatelet(selectors.forView(collector.View())))
	work := make([]relationWork, 0, len(children))
	for _, child := range children {
		child := child
		work = append(work, relationWork{
			run: func(ctx context.Context) error {
				return s.bindRelationScheduled(ctx, scheduler, session, input, binder, selectors, child)
			},
			skip: child.Unlock,
		})
	}
	return scheduler.Run(work)
}

func (s *Service) bindRelationScheduled(ctx context.Context, scheduler *relationScheduler, session *Session, input reflect.Value, binder xhandler.Binder, selectors invocationSelectors, child *rcollector.Collector) (err error) {
	defer child.Unlock()
	childView := child.View()
	childPlan := session.Artifact.ViewPlanFor(childView)
	if childPlan == nil {
		return fmt.Errorf("reader view plan is not compiled for relation view %s", viewName(childView))
	}
	placeholders, compositeValues, columns := child.ParentPlaceholders()
	if childView.Spec.Source == nil || (childView.Spec.Source.SQL == "" && childView.Spec.Source.Table == "") || emptyRelationParents(columns, placeholders, compositeValues) {
		child.BootstrapFromParentHolder()
		child.Fetched()
		if err := s.bindRelationsScheduled(scheduler, session, input, binder, selectors, child); err != nil {
			return err
		}
		return child.RebindToParent()
	}
	session.recorder.Pending(session.pendingScope, 1)
	defer session.recorder.Pending(session.pendingScope, -1)
	read := session.beginView(ctx, childView)
	read.collector = child
	defer read.finish(&err)
	connection, err := viewConnection(ctx, session, childPlan)
	if err != nil {
		return err
	}
	reader := relationRead{
		service: s, ctx: ctx, session: session, input: input, binder: binder,
		selector: selectors, child: child, plan: childPlan, connection: connection, read: read,
	}
	if err := reader.Read(placeholders, compositeValues, columns); err != nil {
		return err
	}
	read.done(child.Len(), nil)
	child.BootstrapFromParentHolder()
	child.Fetched()
	if err := s.bindRelationsScheduled(scheduler, session, input, binder, selectors, child); err != nil {
		return err
	}
	return child.RebindToParent()
}

func (s *Service) relationConcurrency() int {
	if s == nil || s.relationFetchConcurrency <= 0 {
		return defaultRelationFetchConcurrency
	}
	return s.relationFetchConcurrency
}

func viewConnection(ctx context.Context, session *Session, plan *ViewPlan) (dsql.Connection, error) {
	if session == nil || session.SQL == nil {
		return dsql.Connection{}, fmt.Errorf("reader SQL component is required")
	}
	if plan == nil || plan.View == nil {
		return dsql.Connection{}, fmt.Errorf("reader view plan is required")
	}
	connection, err := session.SQL.Resolve(ctx, plan.Connector)
	if err != nil {
		return dsql.Connection{}, fmt.Errorf("resolve view %s connector %s: %w", viewName(plan.View), plan.Connector, err)
	}
	return connection, nil
}

func viewName(view *data.View) string {
	if view == nil || strings.TrimSpace(view.Spec.Name) == "" {
		return "<root>"
	}
	return view.Spec.Name
}

func toAnySlice[T any](input []T) []any {
	if len(input) == 0 {
		return nil
	}
	result := make([]any, len(input))
	for i, item := range input {
		result[i] = item
	}
	return result
}

func emptyRelationParents(columns []string, placeholders []interface{}, compositeValues [][]interface{}) bool {
	if len(columns) == 0 {
		return false
	}
	if len(columns) > 1 {
		return len(compositeValues) == 0
	}
	return len(placeholders) == 0
}

func unmappedResolver(collector *rcollector.Collector) sqlxio.Resolve {
	return func(column sqlxio.Column) func(pointer unsafe.Pointer) interface{} {
		if collector == nil {
			return nil
		}
		return collector.Resolve(unmappedColumn{column: column})
	}
}

type unmappedColumn struct {
	column sqlxio.Column
}

func (u unmappedColumn) Name() string {
	return u.column.Name()
}

func (u unmappedColumn) ScanType() reflect.Type {
	return u.column.ScanType()
}

func selectorStatelet(selector *xstate.Selector) *rcollector.Statelet {
	if selector == nil {
		return nil
	}
	columns := projectionSelection(selector)
	if len(columns) == 0 {
		return nil
	}
	return &rcollector.Statelet{Columns: columns}
}

func viewProjection(view *data.View, selector *xstate.Selector) []string {
	if view == nil {
		return nil
	}
	selection := projectionSelection(selector)
	if len(selection) == 0 {
		return nil
	}
	relationSelected := map[*data.Relation]bool{}
	for _, item := range selection {
		for _, relation := range view.Relations {
			if relation != nil && strings.EqualFold(strings.TrimSpace(item), strings.TrimSpace(relation.Holder)) {
				relationSelected[relation] = true
			}
		}
	}
	seen := map[string]bool{}
	var result []string
	appendName := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		result = append(result, name)
	}
	for _, item := range selection {
		if relationHolderSelected(view.Relations, item) {
			continue
		}
		appendName(item)
	}
	for _, relation := range view.Relations {
		if relation == nil || !relationSelected[relation] {
			continue
		}
		for _, link := range relation.On {
			if link == nil {
				continue
			}
			appendName(link.OutputColumn())
		}
	}
	return result
}

func relationProjection(view *data.View, selector *xstate.Selector, relation *data.Relation) []string {
	result := viewProjection(view, selector)
	if len(result) == 0 || relation == nil || relation.Of == nil {
		return result
	}
	appendName := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		for _, existing := range result {
			if strings.EqualFold(strings.TrimSpace(existing), name) {
				return
			}
		}
		result = append(result, name)
	}
	for _, link := range relation.Of.On {
		if link != nil {
			appendName(link.OutputColumn())
		}
	}
	return result
}

func relationHolderSelected(relations []*data.Relation, item string) bool {
	for _, relation := range relations {
		if relation != nil && strings.EqualFold(strings.TrimSpace(item), strings.TrimSpace(relation.Holder)) {
			return true
		}
	}
	return false
}

func projectionSelection(selector *xstate.Selector) []string {
	if selector == nil {
		return nil
	}
	seen := map[string]bool{}
	result := make([]string, 0, len(selector.Columns)+len(selector.Fields))
	for _, item := range append(append([]string(nil), selector.Columns...), selector.Fields...) {
		item = strings.TrimSpace(item)
		key := strings.ToLower(item)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, item)
	}
	return result
}

func convertCollectorResult(source reflect.Value, targetType reflect.Type) (reflect.Value, error) {
	if !source.IsValid() {
		return reflect.Zero(targetType), nil
	}
	if source.Type().AssignableTo(targetType) {
		return source, nil
	}
	if source.Kind() != reflect.Slice {
		return reflect.Value{}, fmt.Errorf("reader result %s cannot populate %s", source.Type(), targetType)
	}
	if targetType.Kind() != reflect.Slice {
		return convertCollectorSingle(source, targetType)
	}
	return convertCollectorSlice(source, targetType), nil
}

func convertCollectorSingle(source reflect.Value, targetType reflect.Type) (reflect.Value, error) {
	switch source.Len() {
	case 0:
		return reflect.Zero(targetType), nil
	case 1:
		if converted, ok := convertCollectorItem(source.Index(0), targetType); ok {
			return converted, nil
		}
		return reflect.Value{}, fmt.Errorf("reader row %s cannot populate %s", source.Index(0).Type(), targetType)
	default:
		return reflect.Value{}, fmt.Errorf("reader returned %d rows for one %s value", source.Len(), targetType)
	}
}

func convertCollectorSlice(source reflect.Value, targetType reflect.Type) reflect.Value {
	if !source.IsValid() {
		return reflect.Zero(targetType)
	}
	if source.Type().AssignableTo(targetType) {
		return source
	}
	if source.Kind() != reflect.Slice || targetType.Kind() != reflect.Slice {
		return reflect.Value{}
	}
	result := reflect.MakeSlice(targetType, 0, source.Len())
	elemType := targetType.Elem()
	for i := 0; i < source.Len(); i++ {
		if converted, ok := convertCollectorItem(source.Index(i), elemType); ok {
			result = reflect.Append(result, converted)
		}
	}
	return result
}

func convertCollectorItem(item reflect.Value, targetType reflect.Type) (reflect.Value, bool) {
	if item.Type().AssignableTo(targetType) {
		return item, true
	}
	if item.Type().ConvertibleTo(targetType) {
		return item.Convert(targetType), true
	}
	if targetType.Kind() == reflect.Ptr {
		targetElem := targetType.Elem()
		if item.Kind() == reflect.Ptr && !item.IsNil() {
			item = item.Elem()
		}
		if item.Type().AssignableTo(targetElem) {
			pointer := reflect.New(targetElem)
			pointer.Elem().Set(item)
			return pointer, true
		}
		if item.Type().ConvertibleTo(targetElem) {
			pointer := reflect.New(targetElem)
			pointer.Elem().Set(item.Convert(targetElem))
			return pointer, true
		}
	}
	if item.Kind() == reflect.Ptr && !item.IsNil() {
		item = item.Elem()
		if item.Type().AssignableTo(targetType) {
			return item, true
		}
		if item.Type().ConvertibleTo(targetType) {
			return item.Convert(targetType), true
		}
	}
	return reflect.Value{}, false
}
