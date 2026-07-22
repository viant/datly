// Package uow owns one invocation-scoped mutation unit of work.
package uow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

type Relation uint8

const (
	RelationRoot Relation = iota
	RelationBinding
	RelationImperative
)

var (
	ErrCompleted           = errors.New("mutation unit of work is completed")
	ErrFailed              = errors.New("mutation unit of work has failed")
	ErrFrameSealed         = errors.New("component mutation frame is sealed")
	ErrBindingFlush        = errors.New("cannot flush a binding child while its ancestor is open")
	ErrTransactionConflict = errors.New("conflicting transaction for database")
	ErrTransactionAccess   = errors.New("direct transaction access is unavailable in invocation mode")
)

type contextKey struct{}
type bindingOrderKey struct{}

type carrier struct {
	scope    *Scope
	frame    *Frame
	relation Relation
	order    string
}

// Scope coordinates component frames and database transactions for one root operation.
type Scope struct {
	mu        sync.Mutex
	flushMu   sync.Mutex
	root      *Frame
	databases map[*sql.DB]*databaseUnit
	completed bool
	failed    error
	nextFrame uint64
	nextOp    uint64
	nextDB    uint64
}

// Frame owns the mutation timeline of one component invocation.
type Frame struct {
	scope       *Scope
	id          uint64
	name        string
	relation    Relation
	order       string
	parent      *Frame
	open        bool
	timeline    []timelineEntry
	bindings    []*Frame
	nextBinding uint64
}

// DebugLabel identifies a frame in diagnostics without exposing its mutable
// timeline or ownership internals.
func (f *Frame) DebugLabel() string {
	if f == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d:%s", f.id, f.name)
}

type timelineEntry struct {
	operation *Operation
	child     *Frame
}

// Buffer associates statements with one component frame and database source.
type Buffer struct {
	scope        *Scope
	frame        *Frame
	resolveDB    func(context.Context) (*sql.DB, error)
	externalTx   *sql.Tx
	execute      func(context.Context, *sql.Tx, any) error
	executeBatch func(context.Context, *sql.Tx, []any) error
}

// Operation is one ordered buffered mutation.
type Operation struct {
	id       uint64
	buffer   *Buffer
	value    any
	table    string
	executed bool
	reserved bool
}

type databaseUnit struct {
	mu       sync.Mutex
	db       *sql.DB
	tx       *sql.Tx
	external bool
	failed   error
	order    uint64
}

// NewRoot creates a new root scope and installs it in ctx.
func NewRoot(ctx context.Context, name string) (context.Context, *Scope, *Frame) {
	scope := &Scope{databases: map[*sql.DB]*databaseUnit{}, nextFrame: 1}
	frame := &Frame{scope: scope, id: 1, name: name, relation: RelationRoot, open: true}
	scope.root = frame
	return context.WithValue(ctx, contextKey{}, &carrier{scope: scope, frame: frame}), scope, frame
}

// FromContext returns the active scope and frame.
func FromContext(ctx context.Context) (*Scope, *Frame, bool) {
	if ctx == nil {
		return nil, nil, false
	}
	value, _ := ctx.Value(contextKey{}).(*carrier)
	if value == nil || value.scope == nil || value.frame == nil {
		return nil, nil, false
	}
	return value.scope, value.frame, true
}

// Propagate copies only Datly's private invocation carrier from source to
// destination. Dispatcher closures use it so caller cancellation is retained
// while a nested session cannot accidentally lose the active unit of work.
func Propagate(source, destination context.Context) context.Context {
	if destination == nil {
		destination = context.Background()
	}
	if source == nil {
		return destination
	}
	value, _ := source.Value(contextKey{}).(*carrier)
	if value == nil {
		return destination
	}
	return context.WithValue(destination, contextKey{}, value)
}

// PrepareChild marks the next component dispatch as a child of the active frame.
func PrepareChild(ctx context.Context, relation Relation, order string) context.Context {
	scope, frame, ok := FromContext(ctx)
	if !ok {
		return ctx
	}
	// A dispatcher/session may outlive the component frame in which it was
	// created. A subsequent explicit child dispatch is a sibling of that sealed
	// invocation, not an attempt to reopen it. Attach it to the nearest open
	// ancestor so repeated imperative dispatch remains ordered in the shared
	// scope. Enter called directly with the stale context still rejects reuse.
	scope.mu.Lock()
	for frame != nil && !frame.open && frame.parent != nil {
		frame = frame.parent
	}
	scope.mu.Unlock()
	return context.WithValue(ctx, contextKey{}, &carrier{scope: scope, frame: frame, relation: relation, order: order})
}

// WithBindingOrder records the declaration order reserved by the parameter
// resolver before concurrently evaluating component-valued bindings.
func WithBindingOrder(ctx context.Context, order string) context.Context {
	return context.WithValue(ctx, bindingOrderKey{}, order)
}

// WithBindingOrderIndex extends a reserved parameter slot with the authored
// index of a concurrently resolved repeated item.
func WithBindingOrderIndex(ctx context.Context, index int) context.Context {
	parent := BindingOrder(ctx)
	return WithBindingOrder(ctx, fmt.Sprintf("%s/%020d", parent, index+1))
}

// BindingOrder returns a previously reserved binding declaration order.
func BindingOrder(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	order, _ := ctx.Value(bindingOrderKey{}).(string)
	return order
}

// ReserveBindingOrder allocates a stable frame-wide declaration slot before
// a binding resolver goroutine is started.
func ReserveBindingOrder(ctx context.Context) (string, error) {
	scope, frame, ok := FromContext(ctx)
	if !ok {
		return "", nil
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	if scope.completed {
		return "", ErrCompleted
	}
	if scope.failed != nil {
		return "", errors.Join(ErrFailed, scope.failed)
	}
	if !frame.open {
		return "", ErrFrameSealed
	}
	frame.nextBinding++
	return fmt.Sprintf("%020d", frame.nextBinding), nil
}

// Enter creates a child frame when ctx was prepared for dispatch. Otherwise it
// returns the current root frame, or creates a root for an external entry.
func Enter(ctx context.Context, name string) (context.Context, *Scope, *Frame, bool, error) {
	value, _ := ctx.Value(contextKey{}).(*carrier)
	if value == nil || value.scope == nil || value.frame == nil {
		ctx, scope, frame := NewRoot(ctx, name)
		return ctx, scope, frame, true, nil
	}
	scope := value.scope
	scope.mu.Lock()
	if scope.completed {
		if value.relation == RelationRoot && value.frame.parent == nil {
			scope.mu.Unlock()
			ctx, freshScope, freshFrame := NewRoot(ctx, name)
			return ctx, freshScope, freshFrame, true, nil
		}
		scope.mu.Unlock()
		return nil, nil, nil, false, ErrCompleted
	}
	if scope.failed != nil {
		err := scope.failed
		scope.mu.Unlock()
		return nil, nil, nil, false, errors.Join(ErrFailed, err)
	}
	if value.relation == RelationRoot {
		if !value.frame.open {
			scope.mu.Unlock()
			return nil, nil, nil, false, ErrFrameSealed
		}
		scope.mu.Unlock()
		return ctx, scope, value.frame, false, nil
	}
	if !value.frame.open {
		scope.mu.Unlock()
		return nil, nil, nil, false, ErrFrameSealed
	}
	scope.nextFrame++
	child := &Frame{
		scope: scope, id: scope.nextFrame, name: name, relation: value.relation,
		order: value.order, parent: value.frame, open: true,
	}
	if value.relation == RelationBinding {
		value.frame.bindings = append(value.frame.bindings, child)
		sort.SliceStable(value.frame.bindings, func(i, j int) bool {
			return value.frame.bindings[i].order < value.frame.bindings[j].order
		})
	} else {
		value.frame.timeline = append(value.frame.timeline, timelineEntry{child: child})
	}
	ctx = context.WithValue(ctx, contextKey{}, &carrier{scope: scope, frame: child})
	scope.mu.Unlock()
	return ctx, scope, child, false, nil
}

// IsCompleted reports whether root completion has run.
func (s *Scope) IsCompleted() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completed
}

// AdoptTransaction registers a caller-owned transaction for db before child
// binding evaluation can lazily create a local transaction for that database.
func (s *Scope) AdoptTransaction(db *sql.DB, tx *sql.Tx) error {
	if s == nil || db == nil || tx == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completed {
		return ErrCompleted
	}
	if existing := s.databases[db]; existing != nil {
		if existing.tx != tx {
			return ErrTransactionConflict
		}
		return nil
	}
	s.nextDB++
	s.databases[db] = &databaseUnit{db: db, tx: tx, external: true, order: s.nextDB}
	return nil
}

// Seal closes a component frame to new semantic children.
func (f *Frame) Seal() {
	if f == nil || f.scope == nil {
		return
	}
	f.scope.mu.Lock()
	f.open = false
	f.scope.mu.Unlock()
}

// NewBuffer creates a frame-scoped statement buffer.
func (f *Frame) NewBuffer(resolveDB func(context.Context) (*sql.DB, error), externalTx *sql.Tx, execute func(context.Context, *sql.Tx, any) error) *Buffer {
	if f == nil {
		return nil
	}
	return &Buffer{scope: f.scope, frame: f, resolveDB: resolveDB, externalTx: externalTx, execute: execute}
}

// SetBatchExecutor enables batching for contiguous operations that expose the
// same non-empty BatchKey. Ordering barriers are never crossed.
func (b *Buffer) SetBatchExecutor(execute func(context.Context, *sql.Tx, []any) error) {
	if b != nil {
		b.executeBatch = execute
	}
}

// Append adds one operation at the exact current position in the frame timeline.
func (b *Buffer) Append(value any) error {
	if b == nil || b.scope == nil || b.frame == nil {
		return fmt.Errorf("mutation buffer is not configured")
	}
	s := b.scope
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completed {
		return ErrCompleted
	}
	if s.failed != nil {
		return errors.Join(ErrFailed, s.failed)
	}
	if !b.frame.open {
		return ErrFrameSealed
	}
	s.nextOp++
	op := &Operation{id: s.nextOp, buffer: b, value: value, table: operationTable(value)}
	b.frame.timeline = append(b.frame.timeline, timelineEntry{operation: op})
	return nil
}

// Reconcile replaces this buffer's observed entries with the exact execution
// sequence produced by template materialization while retaining child markers.
func (b *Buffer) Reconcile(values []any) error {
	if b == nil || b.scope == nil || b.frame == nil {
		return fmt.Errorf("mutation buffer is not configured")
	}
	s := b.scope
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completed {
		return ErrCompleted
	}
	if s.failed != nil {
		return errors.Join(ErrFailed, s.failed)
	}
	if !b.frame.open {
		return ErrFrameSealed
	}
	existing := make([]*Operation, 0)
	for _, entry := range b.frame.timeline {
		if entry.operation != nil && entry.operation.buffer == b && !entry.operation.executed {
			existing = append(existing, entry.operation)
		}
	}
	desired := make([]*Operation, 0, len(values))
	used := make([]bool, len(existing))
	for _, value := range values {
		var op *Operation
		for i, candidate := range existing {
			if !used[i] && sameValue(candidate.value, value) {
				op = candidate
				used[i] = true
				break
			}
		}
		if op == nil {
			s.nextOp++
			op = &Operation{id: s.nextOp, buffer: b, value: value, table: operationTable(value)}
		}
		desired = append(desired, op)
	}
	result := make([]timelineEntry, 0, len(b.frame.timeline)+len(desired))
	next := 0
	insertAt := -1
	for _, entry := range b.frame.timeline {
		if entry.operation != nil && entry.operation.buffer == b && !entry.operation.executed {
			if next < len(desired) {
				result = append(result, timelineEntry{operation: desired[next]})
				next++
				insertAt = len(result)
			}
			continue
		}
		result = append(result, entry)
	}
	if insertAt < 0 {
		insertAt = len(result)
	}
	if next < len(desired) {
		tail := append([]timelineEntry(nil), result[insertAt:]...)
		result = result[:insertAt]
		for ; next < len(desired); next++ {
			result = append(result, timelineEntry{operation: desired[next]})
		}
		result = append(result, tail...)
	}
	b.frame.timeline = result
	return nil
}

// Flush executes the causal prefix ending at the requested table for b's database.
func (b *Buffer) Flush(ctx context.Context, table string) error {
	if b == nil || b.scope == nil {
		return fmt.Errorf("mutation buffer is not configured")
	}
	return b.scope.flush(ctx, b.frame, b, table)
}

// UseTransaction serializes work which must share this buffer's database
// transaction, including identity sequencing and buffered DML execution.
func (b *Buffer) UseTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	if b == nil || b.scope == nil {
		return fmt.Errorf("mutation buffer is not configured")
	}
	b.scope.flushMu.Lock()
	defer b.scope.flushMu.Unlock()
	b.scope.mu.Lock()
	if b.scope.completed {
		b.scope.mu.Unlock()
		return ErrCompleted
	}
	if b.scope.failed != nil {
		err := b.scope.failed
		b.scope.mu.Unlock()
		return errors.Join(ErrFailed, err)
	}
	b.scope.mu.Unlock()
	db, err := b.resolveDB(ctx)
	if err != nil {
		return err
	}
	unit, err := b.scope.database(ctx, db, b.externalTx)
	if err != nil {
		return err
	}
	unit.mu.Lock()
	defer unit.mu.Unlock()
	if unit.failed != nil {
		return errors.Join(ErrFailed, unit.failed)
	}
	if err = fn(unit.tx); err != nil {
		unit.failed = err
		b.scope.mu.Lock()
		b.scope.failed = err
		b.scope.mu.Unlock()
	}
	return err
}

// Transaction returns the database transaction owned by the invocation scope.
// The caller may issue work through it, but must not commit or roll it back;
// completion remains the responsibility of the root scope.
func (b *Buffer) Transaction(ctx context.Context) (*sql.Tx, error) {
	if b == nil || b.scope == nil {
		return nil, fmt.Errorf("mutation buffer is not configured")
	}
	b.scope.mu.Lock()
	if b.scope.completed {
		b.scope.mu.Unlock()
		return nil, ErrCompleted
	}
	if b.scope.failed != nil {
		err := b.scope.failed
		b.scope.mu.Unlock()
		return nil, errors.Join(ErrFailed, err)
	}
	b.scope.mu.Unlock()
	db, err := b.resolveDB(ctx)
	if err != nil {
		return nil, err
	}
	unit, err := b.scope.database(ctx, db, b.externalTx)
	if err != nil {
		return nil, err
	}
	unit.mu.Lock()
	defer unit.mu.Unlock()
	if unit.failed != nil {
		return nil, errors.Join(ErrFailed, unit.failed)
	}
	return unit.tx, nil
}

// Finish drains and completes locally owned transactions at the root boundary.
func (s *Scope) Finish(ctx context.Context, cause error) error {
	if s == nil {
		return cause
	}
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.mu.Lock()
	if s.completed {
		s.mu.Unlock()
		return errors.Join(cause, ErrCompleted)
	}
	s.completed = true
	sealFrameTree(s.root)
	s.mu.Unlock()

	if cause == nil {
		cause = s.flushLocked(ctx, s.root, nil, "")
	}
	if cause != nil {
		return errors.Join(cause, s.rollbackLocal())
	}
	return s.commitLocal()
}

func sealFrameTree(frame *Frame) {
	if frame == nil {
		return
	}
	frame.open = false
	for _, entry := range frame.timeline {
		if entry.child != nil {
			sealFrameTree(entry.child)
		}
	}
	for _, child := range frame.bindings {
		sealFrameTree(child)
	}
}

func (s *Scope) flush(ctx context.Context, caller *Frame, target *Buffer, table string) error {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.mu.Lock()
	if s.completed {
		s.mu.Unlock()
		return ErrCompleted
	}
	s.mu.Unlock()
	return s.flushLocked(ctx, caller, target, table)
}

func (s *Scope) flushLocked(ctx context.Context, caller *Frame, target *Buffer, table string) error {
	s.mu.Lock()
	if s.failed != nil {
		err := s.failed
		s.mu.Unlock()
		return errors.Join(ErrFailed, err)
	}
	if hasOpenAncestorOfBinding(caller) {
		s.mu.Unlock()
		return ErrBindingFlush
	}
	ordered := flatten(s.root)
	s.mu.Unlock()

	var targetDB *sql.DB
	var err error
	if target != nil {
		targetDB, err = target.resolveDB(ctx)
		if err != nil {
			return err
		}
	}
	selected := make([]*Operation, 0, len(ordered))
	lastMatch := -1
	for _, op := range ordered {
		if op.executed {
			continue
		}
		db, resolveErr := op.buffer.resolveDB(ctx)
		if resolveErr != nil {
			return resolveErr
		}
		if targetDB != nil && db != targetDB {
			continue
		}
		selected = append(selected, op)
		if target == nil || (op.buffer == target && (table == "" || strings.EqualFold(op.table, table))) {
			lastMatch = len(selected) - 1
		}
	}
	if lastMatch < 0 {
		return nil
	}
	selected = selected[:lastMatch+1]

	s.mu.Lock()
	for _, op := range selected {
		if op.executed || op.reserved {
			s.mu.Unlock()
			return fmt.Errorf("operation %d is already reserved or executed", op.id)
		}
		op.reserved = true
	}
	s.mu.Unlock()

	for index := 0; index < len(selected); {
		group := selected[index : index+1]
		key := operationBatchKey(selected[index].value)
		if key != "" && selected[index].buffer.executeBatch != nil {
			end := index + 1
			for end < len(selected) && selected[end].buffer == selected[index].buffer && operationBatchKey(selected[end].value) == key {
				end++
			}
			group = selected[index:end]
		}
		if err = s.executeGroup(ctx, group); err != nil {
			s.mu.Lock()
			s.failed = err
			for _, pending := range selected {
				pending.reserved = false
			}
			s.mu.Unlock()
			return err
		}
		s.mu.Lock()
		for _, op := range group {
			op.executed = true
			op.reserved = false
		}
		s.mu.Unlock()
		index += len(group)
	}
	return nil
}

func hasOpenAncestorOfBinding(caller *Frame) bool {
	for frame := caller; frame != nil; frame = frame.parent {
		if frame.relation != RelationBinding {
			continue
		}
		for ancestor := frame.parent; ancestor != nil; ancestor = ancestor.parent {
			if ancestor.open {
				return true
			}
		}
	}
	return false
}

func (s *Scope) executeGroup(ctx context.Context, operations []*Operation) error {
	if len(operations) == 0 {
		return nil
	}
	buffer := operations[0].buffer
	db, err := buffer.resolveDB(ctx)
	if err != nil {
		return err
	}
	unit, err := s.database(ctx, db, buffer.externalTx)
	if err != nil {
		return err
	}
	unit.mu.Lock()
	defer unit.mu.Unlock()
	if unit.failed != nil {
		return unit.failed
	}
	if len(operations) > 1 && buffer.executeBatch != nil {
		values := make([]any, len(operations))
		for index, operation := range operations {
			values[index] = operation.value
		}
		err = buffer.executeBatch(ctx, unit.tx, values)
	} else {
		err = buffer.execute(ctx, unit.tx, operations[0].value)
	}
	if err != nil {
		unit.failed = err
		return err
	}
	return nil
}

func (s *Scope) database(ctx context.Context, db *sql.DB, external *sql.Tx) (*databaseUnit, error) {
	if db == nil {
		return nil, fmt.Errorf("mutation database is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing := s.databases[db]; existing != nil {
		if external != nil && existing.tx != external {
			return nil, ErrTransactionConflict
		}
		return existing, nil
	}
	s.nextDB++
	unit := &databaseUnit{db: db, tx: external, external: external != nil, order: s.nextDB}
	if unit.tx == nil {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		unit.tx = tx
	}
	s.databases[db] = unit
	return unit, nil
}

func (s *Scope) rollbackLocal() error {
	s.mu.Lock()
	units := make([]*databaseUnit, 0, len(s.databases))
	for _, unit := range s.databases {
		units = append(units, unit)
	}
	s.mu.Unlock()
	sort.Slice(units, func(i, j int) bool { return units[i].order > units[j].order })
	var result error
	for _, unit := range units {
		if !unit.external && unit.tx != nil {
			result = errors.Join(result, unit.tx.Rollback())
		}
	}
	return result
}

func (s *Scope) commitLocal() error {
	s.mu.Lock()
	units := make([]*databaseUnit, 0, len(s.databases))
	for _, unit := range s.databases {
		units = append(units, unit)
	}
	s.mu.Unlock()
	sort.Slice(units, func(i, j int) bool { return units[i].order < units[j].order })
	for index, unit := range units {
		if !unit.external && unit.tx != nil {
			if err := unit.tx.Commit(); err != nil {
				result := err
				for _, pending := range units[index+1:] {
					if !pending.external && pending.tx != nil {
						result = errors.Join(result, pending.tx.Rollback())
					}
				}
				return result
			}
		}
	}
	return nil
}

func flatten(frame *Frame) []*Operation {
	if frame == nil {
		return nil
	}
	var result []*Operation
	for _, entry := range frame.timeline {
		if entry.operation != nil {
			result = append(result, entry.operation)
		}
		if entry.child != nil {
			result = append(result, flatten(entry.child)...)
		}
	}
	for _, child := range frame.bindings {
		result = append(result, flatten(child)...)
	}
	return result
}

type tableNamer interface{ TableName() string }
type batchKeyer interface{ BatchKey() string }

func operationTable(value any) string {
	if named, ok := value.(tableNamer); ok {
		return named.TableName()
	}
	return ""
}

func operationBatchKey(value any) string {
	if keyed, ok := value.(batchKeyer); ok {
		return keyed.BatchKey()
	}
	return ""
}

func sameValue(a, b any) bool {
	if a == nil || b == nil {
		return a == b
	}
	av, bv := reflect.ValueOf(a), reflect.ValueOf(b)
	if av.Type() != bv.Type() {
		return false
	}
	if av.Kind() == reflect.Ptr || av.Kind() == reflect.Map || av.Kind() == reflect.Slice || av.Kind() == reflect.Func || av.Kind() == reflect.Chan {
		return av.Pointer() == bv.Pointer()
	}
	return av.Type().Comparable() && av.Interface() == bv.Interface()
}
