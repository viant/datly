package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/viant/datly/internal/drainowner"
	"reflect"
	"sync"

	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/metadata/info"
	"github.com/viant/xdatly/connector"
	xhandler "github.com/viant/xdatly/handler"
)

type dataScopeContextKey struct{}

type dataScope struct {
	nativeInvocation         *drainowner.Invocation
	journalFrame             *drainowner.Frame
	orderedCompletion        bool
	nativeHandle             drainowner.Handle
	nativeCleanup            bool
	writeEligibility         mutationGuard
	sequenceOnce             sync.Once
	sequenceSource           dexec.DataSource
	sequenceResolved         string
	sequenceError            error
	sequenceStrategy         string
	mu                       sync.Mutex
	source                   dexec.DataSource
	root                     *dataScope
	parent                   *dataScope
	unit                     *dataScope
	units                    []*dataScope
	bySource                 map[any]*dataScope
	requireBufferedOwner     bool
	bufferedScopeEnrolled    bool
	relation                 ComponentRelation
	order                    string
	once                     sync.Once
	data                     xhandler.Data
	err                      error
	completionErr            error
	completion               xhandler.Outcome
	finalizers               []*outcomeFrame
	finalized                bool
	completionStarted        bool
	protectedCompletionOnce  sync.Once
	protectedCompletionUnits []*dataScope
	protectedCompletionErr   error
	guardedExecution         bool
	resolving                sync.WaitGroup
	registering              sync.WaitGroup
	enrollmentMu             sync.Mutex
	guardedFailure           error
	contextReleases          []context.CancelFunc
	connectors               connector.Provider
}

type invocationData interface {
	xhandler.Data
	BeginInvocation() error
	Complete(context.Context, error) error
}

// Guard registration and preflight are capabilities of the same unit owner.
type guardedInvocationData interface {
	invocationData
	invocationDataPreparer
	RegisterExecutionGuard(func(context.Context) error) error
	EnableCapturedExecutionGuards() error
	ValidateExecutionGuards(context.Context) error
	CloseMutationAdmission() error
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
	guard   *mutationGuard
	service rhandler.TransactionSQL
}

func (c transactionSQLCapability) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if err := c.guard.admitStreamingQuery(); err != nil {
		return nil, err
	}
	return c.service.QueryContext(ctx, query, args...)
}

func (c transactionSQLCapability) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if err := c.guard.admitStreamingQuery(); err != nil {
		return guardedQueryErrorRow(ctx, err)
	}
	return c.service.QueryRowContext(ctx, query, args...)
}

func (c transactionSQLCapability) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := c.guard.check("ExecContext"); err != nil {
		return nil, err
	}
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
	return transactionSQLCapability{service: txSQL, guard: p.scope.mutationGuard()}, nil
}

// dmlCapability exposes only buffered writes under the focused DML key.
type dmlCapability struct {
	guard   *mutationGuard
	service xhandler.DML
}

func (c dmlCapability) Dialect(ctx context.Context) (*info.Dialect, error) {
	provider, ok := c.service.(rhandler.DialectProvider)
	if !ok {
		return nil, nil
	}
	return provider.Dialect(ctx)
}

func (c dmlCapability) Insert(tableName string, data any) error {
	if err := c.guard.check("Insert"); err != nil {
		return err
	}
	return c.service.Insert(tableName, data)
}

func (c dmlCapability) Update(tableName string, data any) error {
	if err := c.guard.check("Update"); err != nil {
		return err
	}
	return c.service.Update(tableName, data)
}

func (c dmlCapability) Delete(tableName string, data any) error {
	if err := c.guard.check("Delete"); err != nil {
		return err
	}
	return c.service.Delete(tableName, data)
}

func (c dmlCapability) UpdateWithOptions(tableName string, data any, options ...xhandler.Option) error {
	if err := c.guard.check("UpdateWithOptions"); err != nil {
		return err
	}
	service, ok := c.service.(xhandler.MatchedDML)
	if !ok {
		return fmt.Errorf("atomic matched update is unavailable for %s", tableName)
	}
	return service.UpdateWithOptions(tableName, data, options...)
}

func (c dmlCapability) DeleteWithOptions(tableName string, data any, options ...xhandler.Option) error {
	if err := c.guard.check("DeleteWithOptions"); err != nil {
		return err
	}
	service, ok := c.service.(xhandler.MatchedDML)
	if !ok {
		return fmt.Errorf("atomic matched delete is unavailable for %s", tableName)
	}
	return service.DeleteWithOptions(tableName, data, options...)
}

func (c dmlCapability) UpdateWithCriteria(tableName string, data any, criteria *sqlx.Criteria, options ...xhandler.Option) error {
	if err := c.guard.check("UpdateWithCriteria"); err != nil {
		return err
	}
	service, ok := c.service.(rhandler.CriteriaDML)
	if !ok {
		return fmt.Errorf("native update criteria unavailable for %s", tableName)
	}
	return service.UpdateWithCriteria(tableName, data, criteria, options...)
}

func (c dmlCapability) DeleteWithCriteria(tableName string, data any, criteria *sqlx.Criteria, options ...xhandler.Option) error {
	if err := c.guard.check("DeleteWithCriteria"); err != nil {
		return err
	}
	service, ok := c.service.(rhandler.CriteriaDML)
	if !ok {
		return fmt.Errorf("native delete criteria unavailable for %s", tableName)
	}
	return service.DeleteWithCriteria(tableName, data, criteria, options...)
}

func (c dmlCapability) Execute(dml string, args ...any) error {
	if err := c.guard.check("Execute"); err != nil {
		return err
	}
	return c.service.Execute(dml, args...)
}

// sequencerCapability exposes allocation and optional pending-ID reservation
// under the focused sequencer key, without exposing database ownership.
type sequencerCapability struct {
	guard   *mutationGuard
	service xhandler.Sequencer
}

func (c sequencerCapability) Allocate(ctx context.Context, tableName string, dest any, selector string) error {
	if err := c.guard.check("Allocate"); err != nil {
		return err
	}
	return c.service.Allocate(ctx, tableName, dest, selector)
}

// Reserve forwards the optional reservation hint. Custom allocators without
// this capability retain their allocation contract and final identity guards.
func (c sequencerCapability) Reserve(ctx context.Context, tableName string, dest any, selector string) error {
	if err := c.guard.check("Reserve"); err != nil {
		return err
	}
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
	guard   *mutationGuard
	service xhandler.Flusher
}

func (c flusherCapability) Flush(ctx context.Context, tableName string) error {
	if err := c.guard.check("Flush"); err != nil {
		return err
	}
	return c.service.Flush(ctx, tableName)
}

// dataCapability is the complete public handler data surface. Transaction
// ownership remains an implementation policy of Flush.
type dataCapability struct {
	dmlCapability
	sequencerCapability
	flusherCapability
}

func newHandlerData(service xhandler.Data, guard *mutationGuard) dataCapability {
	return dataCapability{
		dmlCapability:       dmlCapability{service: service, guard: guard},
		sequencerCapability: sequencerCapability{service: service, guard: guard},
		flusherCapability:   flusherCapability{service: service, guard: guard},
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
		child := &dataScope{source: source, root: root, parent: inherited, relation: component.relation, order: component.order}
		root.mu.Lock()
		child.ensureJournalFrameLocked(root)
		root.mu.Unlock()
		return child, false
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

// bufferedLifetimeClosed is an owner lifetime check, not a caller policy switch.
func (s *dataScope) bufferedLifetimeClosed() bool {
	if s == nil {
		return false
	}
	root := s
	if s.root != nil {
		root = s.root
	}
	root.mu.Lock()
	defer root.mu.Unlock()
	return (root.bufferedScopeEnrolled || root.guardedExecution || drainowner.ActivitiesEnrolled(root.nativeInvocation)) && root.completionStarted
}

// enrollBufferedScope keeps caller policy local while making owner lifetime closure monotonic.
func (s *dataScope) nativeIssuerLocked() *drainowner.Invocation {
	if s.nativeInvocation == nil {
		s.nativeInvocation = drainowner.NewInvocation()
	}
	return s.nativeInvocation
}
func (s *dataScope) admitActivity(contexts ...context.Context) (drainowner.Activity, error) {
	root := s
	if s.root != nil {
		root = s.root
	}
	root.mu.Lock()
	defer root.mu.Unlock()
	issuer := root.nativeIssuerLocked()
	if len(contexts) != 0 && drainowner.BindingGroupContext(contexts[0]) {
		return drainowner.AdmitBindingGroupActivity(contexts[0], issuer)
	}
	s.ensureJournalFrameLocked(root)
	if s.err != nil {
		return drainowner.Activity{}, s.err
	}
	return drainowner.AdmitFrameActivity(issuer, s.journalFrame)
}
func (s *dataScope) finishActivity(token drainowner.Activity, cause error) error {
	root := s
	if s.root != nil {
		root = s.root
	}
	root.mu.Lock()
	issuer := root.nativeIssuerLocked()
	root.mu.Unlock()
	return drainowner.FinishActivity(issuer, token, cause)
}
func (s *dataScope) enrollBufferedScope(contexts ...context.Context) error {
	root := s
	if s.root != nil {
		root = s.root
	}
	root.mu.Lock()
	defer root.mu.Unlock()
	issuer := root.nativeIssuerLocked()
	if len(contexts) != 0 && drainowner.BindingGroupContext(contexts[0]) {
		if err := drainowner.BindingGroupAdmission(contexts[0], issuer); err != nil {
			return err
		}
		s.requireBufferedOwner = true
		return nil
	}
	enrollErr := drainowner.EnrollActivities(issuer)
	if bindErr := root.writeEligibility.bindProtectedIssuer(issuer); bindErr != nil {
		enrollErr = errors.Join(enrollErr, bindErr)
	}
	if err := enrollErr; err != nil {
		return err
	}
	s.requireBufferedOwner = true
	root.bufferedScopeEnrolled = true
	if err := issuer.EnableJournal(); err != nil {
		root.guardedFailure = errors.Join(root.guardedFailure, err)
		return err
	}
	return root.orderedAdmissionLocked()
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
	if root.completionStarted {
		err := root.rejectProtectedLocked(fmt.Errorf("invocation completion has already started"))
		root.mu.Unlock()
		return nil, err
	}
	s.ensureJournalFrameLocked(root)
	if err := root.orderedAdmissionLocked(); err != nil {
		root.mu.Unlock()
		return nil, err
	}
	root.resolving.Add(1)
	root.mu.Unlock()
	defer root.resolving.Done()
	s.once.Do(func() {
		defer func() {
			if s.err == nil && s.data != nil {
				unit := s.unit
				if unit == nil {
					unit = s
				}
				if unit.nativeCleanup {
					s.err = unit.nativeHandle.BindFrame(unit.data, s.data, root.nativeInvocation, s.journalFrame)
				}
			}
		}()
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
			handle, claimErr := drainowner.Claim(s.data, root.nativeInvocation)
			if claimErr == nil {
				s.nativeHandle = handle
				s.err = s.attachNativeOwner(root.nativeInvocation)
			} else if errors.Is(claimErr, drainowner.ErrOwner) {
				// Ordinary custom sources retain their existing lifecycle. They do not
				// acquire native authority and cannot later stand in for a native owner.
				if lifecycle, ok := s.data.(invocationData); ok {
					s.err = lifecycle.BeginInvocation()
				}
			} else {
				s.err = claimErr
			}
		}
	})
	if s.err != nil {
		return nil, s.err
	}
	root.mu.Lock()
	if err := root.orderedAdmissionLocked(); err != nil {
		root.mu.Unlock()
		return nil, err
	}
	guarded, failure := root.guardedExecution, root.guardedFailure
	root.mu.Unlock()
	if failure != nil {
		return nil, failure
	}
	if guarded && s.data != nil {
		unit := s.unit
		if unit == nil {
			unit = root
		}
		if err := root.enrollExecutionOwner(unit); err != nil {
			return nil, err
		}
	}
	if s.requireBufferedOwner || root.requireBufferedOwner || s.relation == ComponentBufferedImperative {
		if s.data != nil {
			if _, ok := s.data.(componentData); !ok {
				return nil, fmt.Errorf("buffered component calls require native component journal ownership")
			}
			if _, ok := s.data.(componentDataSealer); !ok {
				return nil, fmt.Errorf("buffered component calls require native component journal sealing")
			}
			unit := s.unit
			if unit == nil {
				unit = root
			}
			if _, ok := unit.data.(invocationData); !ok {
				return nil, fmt.Errorf("buffered component calls require native owner invocation lifecycle")
			}
			if _, ok := unit.data.(invocationDataPreparer); !ok {
				return nil, fmt.Errorf("buffered component calls require native owner completion preparation")
			}
			if _, ok := unit.data.(invocationFinalizationPreparer); !ok {
				return nil, fmt.Errorf("buffered component calls require native owner finalization preparation")
			}
		}
	}
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

func (s *dataScope) transactionForDatabase(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	if s == nil || db == nil {
		return nil, nil
	}
	root := s.root
	if root == nil {
		root = s
	}
	root.mu.Lock()
	unit := root.bySource[db]
	root.mu.Unlock()
	if unit == nil {
		return nil, nil
	}
	data, err := unit.resolve(ctx)
	if err != nil {
		return nil, err
	}
	owned, ok := data.(interface{ InvocationTransaction() (*sql.DB, *sql.Tx) })
	if !ok {
		return nil, nil
	}
	resolvedDB, tx := owned.InvocationTransaction()
	if resolvedDB != db {
		return nil, ErrUnknownDatabaseIdentity
	}
	return tx, nil
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
		err := s.rejectProtectedLocked(fmt.Errorf("invocation completion has already started"))
		s.mu.Unlock()
		return nil, err
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
	unit := &dataScope{source: source, root: s, sequenceStrategy: strategy, journalFrame: s.journalFrame}
	unit.unit = unit
	s.bySource[key] = unit
	s.units = append(s.units, unit)
	s.mu.Unlock()
	return unit, nil
}

func (s *dataScope) seal() {
	if s == nil {
		return
	}
	drainowner.SealFrame(s.journalFrame)
	if s.data == nil {
		return
	}
	if sealer, ok := s.data.(componentDataSealer); ok {
		sealer.SealComponent()
	}
}

func (s *dataScope) providers() []locator.Provider {
	providers := []locator.Provider{
		s.transactionStarterProvider(),
		s.mutationReporterProvider(),
		handlerprovider.New(xhandler.DataKey, func(ctx context.Context) (any, bool, error) {
			data, err := s.resolve(ctx)
			if err != nil || data == nil {
				return nil, false, err
			}
			return newHandlerData(data, s.mutationGuard()), true, nil
		}),
		handlerprovider.New(xhandler.DMLKey, func(ctx context.Context) (any, bool, error) {
			data, err := s.resolve(ctx)
			if err != nil || data == nil {
				return nil, false, err
			}
			return newQueueAwareDMLCapability(data, s.mutationGuard()), true, nil
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
			return sequencerCapability{service: sequencer, guard: s.mutationGuard()}, true, nil
		}),
		handlerprovider.New(xhandler.FlusherKey, func(ctx context.Context) (any, bool, error) {
			data, err := s.resolve(ctx)
			if err != nil || data == nil {
				return nil, false, err
			}
			return flusherCapability{service: data, guard: s.mutationGuard()}, true, nil
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
	var units []*dataScope
	if root.protectedLifetime() {
		var barrierErr error
		units, barrierErr = root.beginProtectedCompletion(ctx)
		if barrierErr != nil {
			handlerErr = errors.Join(handlerErr, barrierErr)
		}
		handlerErr = root.protectedCompletionFailure(handlerErr)
	} else {
		root.mu.Lock()
		if root.nativeInvocation != nil {
			if activityErr := drainowner.CloseActivities(root.nativeInvocation); activityErr != nil {
				handlerErr = errors.Join(handlerErr, activityErr)
			}
		}
		// Completion admission is restricted only for the opt-in outcome owner;
		// ordinary handlers retain their existing post-completion capability usage.
		root.completionStarted = root.completionStarted || len(root.finalizers) != 0 || root.guardedExecution || root.bufferedScopeEnrolled
		closeResolution := root.completionStarted
		for _, frame := range root.finalizers {
			if !frame.finished {
				handlerErr = errors.Join(handlerErr, fmt.Errorf("component %s has not finished before root completion", frame.route))
			}
		}
		root.mu.Unlock()
		if closeResolution {
			root.resolving.Wait()
			root.registering.Wait()
		}
		root.mu.Lock()
		units = append([]*dataScope{root}, root.units...)
		guardedExecution := root.guardedExecution
		guardedFailure := root.guardedFailure
		root.mu.Unlock()
		// Join any source opening already in progress, and close unopened units.
		// An unfinished child cannot create a transaction after rollback begins.
		if closeResolution {
			for _, unit := range units {
				unit.once.Do(func() {})
			}
		}
		// A canonical guard can fail owner resolution before enrollment completes.
		// Its latched failure must veto completion even without enrolled owners.
		if guardedFailure != nil {
			handlerErr = errors.Join(handlerErr, guardedFailure)
		}
		if guardedExecution {
			handlerErr = errors.Join(handlerErr, root.writeEligibility.capturedExecutionFailure())
			root.writeEligibility.closeCompletion()
			for _, unit := range units {
				if unit.data == nil {
					continue
				}
				owner, ok := unit.data.(guardedInvocationData)
				if !ok {
					handlerErr = errors.Join(handlerErr, fmt.Errorf("captured writer execution requires a guarded invocation owner"))
					continue
				}
				handlerErr = errors.Join(handlerErr, completionOperation("close unit mutation admission", owner.CloseMutationAdmission))
			}
		}
	}
	reportingUnits := units
	defer func() { root.recordCompletion(reportingUnits, completionErr) }()
	if root.orderedCompletion {
		units = root.transactionCompletionOrder(units)
	}
	if handlerErr != nil {
		for index := len(units) - 1; index >= 0; index-- {
			handlerErr = units[index].completeUnit(ctx, handlerErr)
		}
		return handlerErr
	}
	if handlerErr = validateUnitExecutionGuards(ctx, units); handlerErr != nil {
		for index := len(units) - 1; index >= 0; index-- {
			handlerErr = units[index].completeUnit(ctx, handlerErr)
		}
		return handlerErr
	}
	if root.orderedCompletion {
		handlerErr = root.prepareOrderedJournal(ctx, units, drainowner.AllPreparation)
	} else {
		for _, unit := range units {
			if handlerErr = unit.prepareUnit(ctx); handlerErr != nil {
				break
			}
		}
	}
	if handlerErr != nil {
		units = root.transactionCompletionOrder(units)
		for index := len(units) - 1; index >= 0; index-- {
			handlerErr = units[index].completeUnit(ctx, handlerErr)
		}
		return handlerErr
	}
	if root.orderedCompletion {
		units = root.transactionCompletionOrder(units)
	}
	if handlerErr = validateUnitExecutionGuards(ctx, units); handlerErr != nil {
		for index := len(units) - 1; index >= 0; index-- {
			handlerErr = units[index].completeUnit(ctx, handlerErr)
		}
		return handlerErr
	}
	if root.protectedLifetime() {
		handlerErr = root.protectedCompletionFailure(handlerErr)
		if handlerErr != nil {
			for index := len(units) - 1; index >= 0; index-- {
				handlerErr = units[index].completeUnit(ctx, handlerErr)
			}
			return handlerErr
		}
	}
	for _, unit := range units {
		handlerErr = unit.completeUnit(ctx, handlerErr)
	}
	if root.protectedLifetime() {
		handlerErr = root.protectedCompletionFailure(handlerErr)
	}
	return handlerErr
}

// Enrollment is monotonic; a caught opt-in failure remains an invocation error.
func (s *dataScope) failGuardedExecution(err error) error {
	root := s.root
	if root == nil {
		root = s
	}
	root.mu.Lock()
	if root.guardedFailure == nil {
		root.guardedFailure = err
	}
	failure := root.guardedFailure
	root.mu.Unlock()
	return failure
}
func (s *dataScope) enrollExecutionOwner(unit *dataScope) error {
	owner, ok := unit.data.(guardedInvocationData)
	if !ok {
		return s.failGuardedExecution(fmt.Errorf("captured writer execution requires an enrolled invocation owner"))
	}
	if err := completionOperation("enroll captured writer owner", owner.EnableCapturedExecutionGuards); err != nil {
		return s.failGuardedExecution(err)
	}
	return nil
}
func (s *dataScope) registerExecutionGuard(ctx context.Context, check func(context.Context) error) error {
	root := s.root
	if root == nil {
		root = s
	}
	root.enrollmentMu.Lock()
	defer root.enrollmentMu.Unlock()
	root.mu.Lock()
	if root.completionStarted {
		err := root.rejectProtectedLocked(fmt.Errorf("invocation completion has already started"))
		root.mu.Unlock()
		return err
	}
	issuer := root.nativeIssuerLocked()
	enrollErr := drainowner.EnrollActivities(issuer)
	if bindErr := root.writeEligibility.bindProtectedIssuer(issuer); bindErr != nil {
		enrollErr = errors.Join(enrollErr, bindErr)
	}
	if err := enrollErr; err != nil {
		root.mu.Unlock()
		return root.failGuardedExecution(err)
	}
	root.guardedExecution = true
	failure := root.writeEligibility.enableCapturedExecution()
	if root.guardedFailure == nil {
		root.guardedFailure = failure
	}
	root.registering.Add(1)
	root.mu.Unlock()
	defer root.registering.Done()
	if failure != nil {
		return root.failGuardedExecution(failure)
	}
	// Each snapshot owner joins its existing once.Do resolution boundary.
	// Later owners enroll before publishing their capabilities.
	root.mu.Lock()
	units := append([]*dataScope{root}, root.units...)
	root.mu.Unlock()
	for _, unit := range units {
		if unit.source == nil && unit.data == nil {
			continue
		}
		if _, err := unit.resolve(ctx); err != nil {
			return root.failGuardedExecution(err)
		}
		if err := root.enrollExecutionOwner(unit); err != nil {
			return err
		}
	}
	root.mu.Lock()
	failure = root.guardedFailure
	root.mu.Unlock()
	if failure != nil {
		return failure
	}
	unit := s.unit
	if unit == nil {
		unit = root
	}
	owner, ok := unit.data.(guardedInvocationData)
	if !ok {
		return root.failGuardedExecution(fmt.Errorf("captured writer execution requires an enrolled invocation owner"))
	}
	if err := completionOperation("register captured writer execution guard", func() error { return owner.RegisterExecutionGuard(check) }); err != nil {
		return root.failGuardedExecution(err)
	}
	return nil
}

// All units preflight before any owned commit, including journals already
// drained by an imperative flush or pre-completion output finalizer.
func validateUnitExecutionGuards(ctx context.Context, units []*dataScope) error {
	for _, unit := range units {
		if unit.err != nil {
			return unit.err
		}
		if guarded, ok := unit.data.(interface{ ValidateExecutionGuards(context.Context) error }); ok {
			if err := completionOperation("captured unit execution guard", func() error { return guarded.ValidateExecutionGuards(ctx) }); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *dataScope) prepareUnit(ctx context.Context) error {
	if s.err != nil {
		return s.err
	}
	if s.data == nil {
		return nil
	}
	if preparer, ok := s.data.(invocationDataPreparer); ok {
		return completionOperation("data completion preparation", func() error {
			if s.nativeCleanup {
				return s.callNativeDrain(ctx, drainowner.AllPreparation, nil)
			}
			return preparer.PrepareCompletion(ctx)
		})
	}
	return nil
}

func (s *dataScope) prepare(ctx context.Context) error {
	root := s
	if root.root != nil {
		root = root.root
	}
	root.mu.Lock()
	buffered := root.bufferedScopeEnrolled
	units := append([]*dataScope{root}, root.units...)
	root.mu.Unlock()
	if buffered {
		return root.prepareProtectedFinalization(ctx)
	}
	for _, unit := range units {
		if unit.err != nil {
			return unit.err
		}
		if preparer, ok := unit.data.(invocationFinalizationPreparer); ok {
			if err := completionOperation("data finalization preparation", func() error {
				if unit.nativeCleanup {
					return unit.callNativeDrain(ctx, drainowner.LocalPreparation, nil)
				}
				return preparer.PrepareFinalization(ctx)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *dataScope) completeUnit(ctx context.Context, handlerErr error) (completionErr error) {
	defer func() {
		if value := recover(); value != nil {
			completionErr = errors.Join(handlerErr, dexec.NewPanicError("data unit completion", value))
		}
		s.mu.Lock()
		s.completionErr = completionErr
		s.mu.Unlock()
	}()
	if s.err != nil {
		cause := errors.Join(handlerErr, s.err)
		if s.nativeCleanup {
			if _, ok := s.data.(invocationData); ok {
				return s.callNativeDrain(ctx, drainowner.Abort, cause)
			}
		}
		return cause
	}
	if s.data == nil {
		return handlerErr
	}
	if lifecycle, ok := s.data.(invocationData); ok {
		if s.nativeCleanup {
			operation := drainowner.Completion
			if handlerErr != nil {
				operation = drainowner.Abort
			}
			err := s.callNativeDrain(ctx, operation, handlerErr)
			if operation == drainowner.Completion && drainowner.IsAdmissionDenied(err) {
				return s.callNativeDrain(ctx, drainowner.Abort, err)
			}
			return err
		}
		return lifecycle.Complete(ctx, handlerErr)
	}
	if handlerErr != nil {
		return handlerErr
	}
	return s.data.Flush(ctx, "")
}

// Native drain authority stays with the actual unit's retained owner handle.
func (s *dataScope) callNativeDrain(ctx context.Context, operation drainowner.Operation, cause error) error {
	root := s
	if root.root != nil {
		root = root.root
	}
	root.mu.Lock()
	issuer := root.nativeInvocation
	root.mu.Unlock()
	return s.nativeHandle.Call(ctx, s.data, issuer, operation, cause)
}

// Recover only the current operation so its owner can continue cleanup and
// record the failure before publishing completion evidence. Never replay it.
func completionOperation(operation string, run func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = dexec.NewPanicError(operation, value)
		}
	}()
	return run()
}

// Attach on the actual unit before any component capability is published.
// Deferred evidence capture also runs when native attachment panics.
func (s *dataScope) attachNativeOwner(issuer *drainowner.Invocation) (err error) {
	defer func() {
		s.nativeCleanup = s.nativeHandle.OwnsAttachment(s.data, issuer)
		if value := recover(); value != nil {
			err = dexec.NewPanicError("native data attachment", value)
		}
	}()
	root := s
	if root.root != nil {
		root = root.root
	}
	root.mu.Lock()
	s.ensureJournalFrameLocked(root)
	root.mu.Unlock()
	err = s.nativeHandle.Attach(s.data, issuer)
	if err == nil {
		err = s.nativeHandle.BindFrame(s.data, s.data, issuer, s.journalFrame)
	}
	return err
}
