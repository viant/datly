package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/xdatly/connector"
	xhandler "github.com/viant/xdatly/handler"
)

type dataScopeContextKey struct{}

type dataScope struct {
	sequenceOnce      sync.Once
	sequenceSource    dexec.DataSource
	sequenceResolved  string
	sequenceError     error
	sequenceStrategy  string
	mu                sync.Mutex
	source            dexec.DataSource
	root              *dataScope
	parent            *dataScope
	unit              *dataScope
	units             []*dataScope
	bySource          map[any]*dataScope
	relation          ComponentRelation
	order             string
	once              sync.Once
	data              xhandler.Data
	err               error
	completionErr     error
	completion        xhandler.Outcome
	finalizers        []*outcomeFrame
	finalized         bool
	completionStarted bool
	contextReleases   []context.CancelFunc
	connectors        connector.Provider
}

type invocationData interface {
	xhandler.Data
	BeginInvocation() error
	Complete(context.Context, error) error
}

type invocationDataPreparer interface {
	PrepareCompletion(context.Context) error
}

type invocationFinalizationPreparer interface {
	PrepareFinalization(context.Context) error
}

type componentData interface {
	xhandler.Data
	ComponentData(string, string) xhandler.Data
}

type componentDataSealer interface {
	SealComponent()
}

type invocationSource interface {
	InvocationKey() any
}

type invocationTransactionSource interface {
	InvocationTransactionKey() any
}

type transactionSQLCapability struct {
	service rhandler.TransactionSQL
}

func (c transactionSQLCapability) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return c.service.QueryContext(ctx, query, args...)
}

func (c transactionSQLCapability) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return c.service.QueryRowContext(ctx, query, args...)
}

func (c transactionSQLCapability) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.service.ExecContext(ctx, query, args...)
}

type transactionSQLProvider struct {
	scope *dataScope
}

func (p transactionSQLProvider) Connector(ctx context.Context, name string) (rhandler.TransactionSQL, error) {
	if p.scope == nil || p.scope.connectors == nil {
		return nil, fmt.Errorf("transaction SQL connector provider is required")
	}
	sources, ok := p.scope.connectors.(dexec.ConnectorDataSourceProvider)
	if !ok {
		return nil, fmt.Errorf("transaction SQL connector %q requires an execution data-source provider", name)
	}
	source, err := sources.ConnectorDataSource(ctx, name)
	if err != nil {
		return nil, err
	}
	key := sourceKey(source)
	if source == nil || key == nil {
		return nil, ErrUnknownDatabaseIdentity
	}
	root := p.scope.root
	if root == nil {
		root = p.scope
	}
	unit, err := root.databaseUnit(ctx, source, key, "")
	if err != nil {
		return nil, err
	}
	data, err := unit.resolve(ctx)
	if err != nil {
		return nil, err
	}
	txSQL, ok := data.(rhandler.TransactionSQL)
	if !ok {
		return nil, fmt.Errorf("transaction SQL is unavailable for connector %q", name)
	}
	return transactionSQLCapability{service: txSQL}, nil
}

// dmlCapability exposes only buffered writes under the focused DML key.
type dmlCapability struct {
	service xhandler.DML
}

func (c dmlCapability) Insert(tableName string, data any) error {
	return c.service.Insert(tableName, data)
}

func (c dmlCapability) Update(tableName string, data any) error {
	return c.service.Update(tableName, data)
}

func (c dmlCapability) Delete(tableName string, data any) error {
	return c.service.Delete(tableName, data)
}

func (c dmlCapability) Execute(dml string, args ...any) error {
	return c.service.Execute(dml, args...)
}

// sequencerCapability exposes allocation and optional pending-ID reservation
// under the focused sequencer key, without exposing database ownership.
type sequencerCapability struct {
	service xhandler.Sequencer
}

func (c sequencerCapability) Allocate(ctx context.Context, tableName string, dest any, selector string) error {
	return c.service.Allocate(ctx, tableName, dest, selector)
}

// Reserve forwards the optional reservation hint. Custom allocators without
// this capability retain their allocation contract and final identity guards.
func (c sequencerCapability) Reserve(ctx context.Context, tableName string, dest any, selector string) error {
	if service, ok := c.service.(interface {
		Reserve(context.Context, string, any, string) error
	}); ok {
		return service.Reserve(ctx, tableName, dest, selector)
	}
	return nil
}

// flusherCapability exposes explicit buffered-write execution without leaking
// the concrete SQL implementation.
type flusherCapability struct {
	service xhandler.Flusher
}

func (c flusherCapability) Flush(ctx context.Context, tableName string) error {
	return c.service.Flush(ctx, tableName)
}

// dataCapability is the complete public handler data surface. Transaction
// ownership remains an implementation policy of Flush.
type dataCapability struct {
	dmlCapability
	sequencerCapability
	flusherCapability
}

func newHandlerData(service xhandler.Data) dataCapability {
	return dataCapability{
		dmlCapability:       dmlCapability{service: service},
		sequencerCapability: sequencerCapability{service: service},
		flusherCapability:   flusherCapability{service: service},
	}
}

func newDataScope(source dexec.DataSource) *dataScope {
	if source == nil {
		return nil
	}
	return &dataScope{source: source}
}

func invocationDataScope(ctx context.Context, source dexec.DataSource) (*dataScope, bool) {
	if inherited, ok := ctx.Value(dataScopeContextKey{}).(*dataScope); ok && inherited != nil {
		root := inherited
		if root.root != nil {
			root = root.root
		}
		component := componentFromContext(ctx)
		return &dataScope{source: source, root: root, parent: inherited, relation: component.relation, order: component.order}, false
	}
	created := newDataScope(source)
	if created != nil {
		created.root = created
		created.unit = created
		created.bySource = map[any]*dataScope{}
		if key := sourceKey(source); key != nil {
			created.bySource[key] = created
		}
	}
	return created, created != nil
}

func withDataScope(ctx context.Context, scope *dataScope) context.Context {
	return context.WithValue(ctx, dataScopeContextKey{}, scope)
}

func completeDataScope(ctx context.Context, scope *dataScope, owned bool, err error) error {
	if scope == nil {
		return err
	}
	if !owned {
		scope.seal()
		return err
	}
	return scope.complete(ctx, err)
}

// finishDataScope preserves structured output for a handler/finalizer error,
// while suppressing output that preceded a root flush or commit failure.
func finishDataScope(ctx context.Context, scope *dataScope, owned bool, result any, operationErr error) (any, error) {
	completionErr := completeDataScope(ctx, scope, owned, operationErr)
	if operationErr != nil {
		if completionIntroducedError(completionErr, operationErr) {
			return result, completionErr
		}
		return result, operationErr
	}
	if completionErr != nil {
		return nil, completionErr
	}
	return result, nil
}

func completionIntroducedError(completionErr, operationErr error) bool {
	if completionErr == nil || completionErr == operationErr {
		return false
	}
	if joined, ok := completionErr.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if completionIntroducedError(child, operationErr) {
				return true
			}
		}
		return false
	}
	if wrapped, ok := completionErr.(interface{ Unwrap() error }); ok {
		return completionIntroducedError(wrapped.Unwrap(), operationErr)
	}
	return !errors.Is(operationErr, completionErr)
}

func (s *dataScope) resolve(ctx context.Context) (xhandler.Data, error) {
	root := s.root
	if root == nil {
		root = s
	}
	root.mu.Lock()
	closed := root.completionStarted
	root.mu.Unlock()
	if closed {
		return nil, fmt.Errorf("invocation completion has already started")
	}
	s.once.Do(func() {
		if s.parent != nil {
			parentData, err := s.parent.resolve(ctx)
			if err != nil {
				s.err = err
				return
			}
			parentUnit := s.parent.unit
			if parentUnit == nil {
				parentUnit = s.root
			}
			key, parentKey := sourceKey(s.source), sourceKey(parentUnit.source)
			if parentUnit.source == nil && s.source != nil {
				if key == nil {
					s.err = ErrUnknownDatabaseIdentity
					return
				}
				unit, err := s.root.databaseUnit(ctx, s.source, key, s.sequenceStrategy)
				if err != nil {
					s.err = err
					return
				}
				s.unit = unit
				s.data, s.err = unit.resolve(ctx)
				if s.err == nil {
					if scoped, ok := s.data.(componentData); ok {
						s.data = scoped.ComponentData(string(s.relation), s.order)
					}
				}
				return
			}
			if s.source != nil && (key == nil || parentKey == nil) && !sameDataSource(s.source, parentUnit.source) {
				s.err = ErrUnknownDatabaseIdentity
				return
			}
			if key != nil && parentKey != nil && key == parentKey {
				if childTx := sourceTransactionKey(s.source); childTx != nil && !sameIdentity(childTx, sourceTransactionKey(parentUnit.source)) {
					s.err = ErrTransactionConflict
					return
				}
			}
			if key != nil && parentKey != nil && key != parentKey {
				unit, unitErr := s.root.databaseUnit(ctx, s.source, key, s.sequenceStrategy)
				if unitErr != nil {
					s.err = unitErr
					return
				}
				s.unit = unit
				unitData, resolveErr := unit.resolve(ctx)
				if resolveErr != nil {
					s.err = resolveErr
					return
				}
				s.data = unitData
				if scoped, ok := unitData.(componentData); ok {
					s.data = scoped.ComponentData(string(s.relation), s.order)
				}
				return
			}
			if err := parentUnit.checkSequenceStrategy(ctx, s.source, s.sequenceStrategy); err != nil {
				s.err = err
				return
			}
			s.unit = parentUnit
			s.data = parentData
			if scoped, ok := parentData.(componentData); ok {
				s.data = scoped.ComponentData(string(s.relation), s.order)
			}
			return
		}
		if s.source == nil {
			return
		} // neutral outcome-aware root; no implicit DB
		_, err := s.effectiveSequenceStrategy(ctx)
		if err != nil {
			s.err = err
			return
		}
		s.data, s.err = s.sequenceSource.Open(ctx)
		if s.err == nil {
			if lifecycle, ok := s.data.(invocationData); ok {
				s.err = lifecycle.BeginInvocation()
			}
		}
	})
	return s.data, s.err
}

func sourceKey(source dexec.DataSource) any {
	if identified, ok := source.(invocationSource); ok {
		key := identified.InvocationKey()
		if key == nil || !reflect.TypeOf(key).Comparable() {
			return nil
		}
		return key
	}
	return nil
}

func sourceTransactionKey(source dexec.DataSource) any {
	if identified, ok := source.(invocationTransactionSource); ok {
		return identified.InvocationTransactionKey()
	}
	return nil
}

func sameDataSource(a, b dexec.DataSource) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	aType, bType := reflect.TypeOf(a), reflect.TypeOf(b)
	return aType == bType && aType.Comparable() && reflect.ValueOf(a).Interface() == reflect.ValueOf(b).Interface()
}

func sameIdentity(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	aType, bType := reflect.TypeOf(a), reflect.TypeOf(b)
	return aType == bType && aType.Comparable() && a == b
}

func (s *dataScope) databaseUnit(ctx context.Context, source dexec.DataSource, key any, strategy string) (*dataScope, error) {
	s.mu.Lock()
	if s.completionStarted {
		s.mu.Unlock()
		return nil, fmt.Errorf("invocation completion has already started")
	}
	if unit := s.bySource[key]; unit != nil {
		if childTx := sourceTransactionKey(source); childTx != nil && !sameIdentity(childTx, sourceTransactionKey(unit.source)) {
			s.mu.Unlock()
			return nil, ErrTransactionConflict
		}
		s.mu.Unlock()
		// Native metadata resolution may perform I/O; never hold the root map lock.
		if err := unit.checkSequenceStrategy(ctx, source, strategy); err != nil {
			return nil, err
		}
		return unit, nil
	}
	unit := &dataScope{source: source, root: s, sequenceStrategy: strategy}
	unit.unit = unit
	s.bySource[key] = unit
	s.units = append(s.units, unit)
	s.mu.Unlock()
	return unit, nil
}

func (s *dataScope) seal() {
	if s == nil || s.data == nil {
		return
	}
	if sealer, ok := s.data.(componentDataSealer); ok {
		sealer.SealComponent()
	}
}

func (s *dataScope) providers() []locator.Provider {
	providers := []locator.Provider{
		s.transactionStarterProvider(),
		handlerprovider.New(xhandler.DataKey, func(ctx context.Context) (any, bool, error) {
			data, err := s.resolve(ctx)
			if err != nil || data == nil {
				return nil, false, err
			}
			return newHandlerData(data), true, nil
		}),
		handlerprovider.New(xhandler.DMLKey, func(ctx context.Context) (any, bool, error) {
			data, err := s.resolve(ctx)
			if err != nil || data == nil {
				return nil, false, err
			}
			return dmlCapability{service: data}, true, nil
		}),
		handlerprovider.New(xhandler.SequencerKey, func(ctx context.Context) (any, bool, error) {
			data, err := s.resolve(ctx)
			if err != nil || data == nil {
				return nil, false, err
			}
			sequencer, ok := data.(xhandler.Sequencer)
			if !ok {
				return nil, false, nil
			}
			return sequencerCapability{service: sequencer}, true, nil
		}),
		handlerprovider.New(xhandler.FlusherKey, func(ctx context.Context) (any, bool, error) {
			data, err := s.resolve(ctx)
			if err != nil || data == nil {
				return nil, false, err
			}
			return flusherCapability{service: data}, true, nil
		}),
	}
	if s.connectors != nil {
		providers = append(providers, handlerprovider.New(rhandler.TransactionSQLCapabilityKey, func(context.Context) (any, bool, error) {
			return transactionSQLProvider{scope: s}, true, nil
		}))
	}
	return providers
}

func (s *dataScope) complete(ctx context.Context, handlerErr error) (completionErr error) {
	root := s
	if root.root != nil {
		root = root.root
	}
	root.mu.Lock()
	// Completion admission is restricted only for the opt-in outcome owner;
	// ordinary handlers retain their existing post-completion capability usage.
	root.completionStarted = len(root.finalizers) != 0
	closeResolution := root.completionStarted
	for _, frame := range root.finalizers {
		if !frame.finished {
			handlerErr = errors.Join(handlerErr, fmt.Errorf("component %s has not finished before root completion", frame.route))
		}
	}
	units := append([]*dataScope{root}, root.units...)
	root.mu.Unlock()
	// Join any source opening already in progress, and close unopened units.
	// An unfinished child cannot create a transaction after rollback begins.
	if closeResolution {
		for _, unit := range units {
			unit.once.Do(func() {})
		}
	}
	defer func() { root.recordCompletion(units, completionErr) }()
	if handlerErr != nil {
		for index := len(units) - 1; index >= 0; index-- {
			handlerErr = units[index].completeUnit(ctx, handlerErr)
		}
		return handlerErr
	}
	for _, unit := range units {
		if handlerErr = unit.prepareUnit(ctx); handlerErr != nil {
			for index := len(units) - 1; index >= 0; index-- {
				handlerErr = units[index].completeUnit(ctx, handlerErr)
			}
			return handlerErr
		}
	}
	for _, unit := range units {
		handlerErr = unit.completeUnit(ctx, handlerErr)
	}
	return handlerErr
}

func (s *dataScope) prepareUnit(ctx context.Context) error {
	if s.err != nil {
		return s.err
	}
	if s.data == nil {
		return nil
	}
	if preparer, ok := s.data.(invocationDataPreparer); ok {
		return preparer.PrepareCompletion(ctx)
	}
	return nil
}

func (s *dataScope) prepare(ctx context.Context) error {
	root := s
	if root.root != nil {
		root = root.root
	}
	root.mu.Lock()
	units := append([]*dataScope{root}, root.units...)
	root.mu.Unlock()
	for _, unit := range units {
		if unit.err != nil {
			return unit.err
		}
		if preparer, ok := unit.data.(invocationFinalizationPreparer); ok {
			if err := preparer.PrepareFinalization(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *dataScope) completeUnit(ctx context.Context, handlerErr error) (completionErr error) {
	defer func() { s.mu.Lock(); s.completionErr = completionErr; s.mu.Unlock() }()
	if s.err != nil {
		return errors.Join(handlerErr, s.err)
	}
	if s.data == nil {
		return handlerErr
	}
	if lifecycle, ok := s.data.(invocationData); ok {
		return lifecycle.Complete(ctx, handlerErr)
	}
	if handlerErr != nil {
		return handlerErr
	}
	return s.data.Flush(ctx, "")
}
