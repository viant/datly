// Package writer provides the metadata-driven built-in mutation handler.
package writer

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/dialectcontext"
	"github.com/viant/datly/internal/drainowner"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	predicate "github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx"
	"github.com/viant/structology"
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
	"github.com/viant/xunsafe"
)

// Metadata is the immutable, component-specific plan interpreted by Handler.
// It contains no generated executable phase code.
type Metadata struct {
	Predicates       *predicate.Program
	Component        *spec.Component
	Operation        string
	InputField       int
	CurrentField     int
	OutputField      int
	EntityType       reflect.Type
	CurrentType      reflect.Type
	Table            string
	Keys             []Field
	Fields           []Field
	Sequence         *Field
	DeleteMarker     *Field
	ConcurrencyToken *Field
	Invariants       map[string][]Field
	HookType         reflect.Type
	Root             *Record
}

// Field is one compiled entity field.
type Field struct {
	Name          string
	Column        string
	Index         []int
	Has           []int
	AutoIncrement bool
	RefDB         string
	RefTable      string
	RefColumn     string
	// marker/markerIndex address this field's presence bit through the
	// entity's structology set-marker (xunsafe backed). Has stays as the
	// reflect fallback for holders without a setMarker tag.
	marker      *structology.Marker
	markerIndex int
}

// compileMarker records the Has marker path and, when the entity declares a
// structology set-marker holder, the direct accessor for this field's bit.
func compileMarker(compiled *Field, has reflect.StructField, marker reflect.StructField, presence *structology.Marker) {
	compiled.Has = append(append([]int(nil), has.Index...), marker.Index...)
	compiled.markerIndex = -1
	if presence != nil {
		if index := presence.Index(compiled.Name); index >= 0 {
			compiled.marker, compiled.markerIndex = presence, index
		}
	}
}

// entityMarker compiles the structology set-marker for an entity type, or nil
// when the type declares no setMarker holder.
func entityMarker(entityType reflect.Type) *structology.Marker {
	if !structology.HasSetMarker(entityType) {
		return nil
	}
	marker, err := structology.NewMarker(entityType, structology.WithNoStrict(true))
	if err != nil {
		return nil
	}
	return marker
}

// Record is one writable role in a component graph.
type Record struct {
	reconciliation           *reconciliationMetadata
	writeEligibility         bool
	RootNullPolicy           string
	NestedNullPolicy         string
	InsertValidationPresence bool
	WriterIdentityPolicy     string
	QueueContract            string
	WriterActionPolicy       string
	OnDeleteNotFound         string
	MutationPredicateGroup   *int
	Name                     string
	Path                     string
	Auxiliary                bool
	Selector                 string
	EntityType               reflect.Type
	CurrentField             int
	Table                    string
	Keys                     []Field
	Fields                   []Field
	Sequence                 *Field
	ScopedSequences          []ScopedSequence
	DeleteMarker             *Field
	ConcurrencyToken         *Field
	Invariants               map[string][]Field
	HookType                 reflect.Type
	Relations                []*Relation
	// positions maps field names to their index in Fields; compiled once.
	positions map[string]int
}

func (r *Record) indexFields() {
	r.positions = make(map[string]int, len(r.Fields))
	for i, field := range r.Fields {
		r.positions[field.Name] = i
	}
}

// position returns the field's index in Fields or -1. Records assembled by
// hand (tests) without compiled positions fall back to a linear scan.
func (r *Record) position(name string) int {
	if r == nil {
		return -1
	}
	if r.positions != nil {
		if pos, ok := r.positions[name]; ok {
			return pos
		}
		return -1
	}
	for i, field := range r.Fields {
		if field.Name == name {
			return i
		}
	}
	return -1
}

// presence is the writer's view of which fields a row supplies. The entity's
// Has marker is the single authority shared with sqlx, godiffer, govalidator
// and structology: a live presence reads it on demand (xunsafe offsets), so
// lifecycle setters are visible without any re-synchronisation pass, even for
// graphs with thousands of field checks. The writer layers two small bitsets
// on top: forced bits for coverage it adds itself (reconciled links, invariant
// backfill, which must not mutate the client's marker) and excluded bits for
// validation coverage. A snapshot presence freezes the marker bits once and
// never reads the working entity.
type presence struct {
	record   *Record
	entity   reflect.Value // addressable struct; invalid for a snapshot
	snapshot []uint64
	forced   []uint64
	excluded []uint64
}

func words(count int) int { return (count + 63) / 64 }

func bit(bits []uint64, pos int) bool { return len(bits) > pos/64 && bits[pos/64]&(1<<(pos%64)) != 0 }

func setBit(bits *[]uint64, count, pos int) {
	if *bits == nil {
		*bits = make([]uint64, words(count))
	}
	(*bits)[pos/64] |= 1 << (pos % 64)
}

// livePresence views the entity's current marker state.
func livePresence(record *Record, entity reflect.Value) *presence {
	return &presence{record: record, entity: entity}
}

// snapshotPresence copies the marker bits once (original presence contract).
func snapshotPresence(record *Record, entity reflect.Value) *presence {
	result := &presence{record: record, snapshot: make([]uint64, words(len(record.Fields)))}
	for pos, field := range record.Fields {
		if supplied(entity, field) {
			result.snapshot[pos/64] |= 1 << (pos % 64)
		}
	}
	return result
}

func (p *presence) hasPos(pos int) bool {
	if p == nil || pos < 0 || pos >= len(p.record.Fields) {
		return false
	}
	if bit(p.excluded, pos) {
		return false
	}
	if bit(p.forced, pos) {
		return true
	}
	if p.entity.IsValid() {
		return supplied(p.entity, p.record.Fields[pos])
	}
	return bit(p.snapshot, pos)
}

// Has implements xhandler.FieldSet.
func (p *presence) Has(name string) bool {
	if p == nil {
		return false
	}
	return p.hasPos(p.record.position(name))
}

// force marks writer-provided coverage without touching the client marker.
func (p *presence) force(name string) {
	if p == nil {
		return
	}
	if pos := p.record.position(name); pos >= 0 {
		setBit(&p.forced, len(p.record.Fields), pos)
	}
}

// excluding returns a coverage view with the named fields removed. A nil
// presence (frames assembled without one) stays nil.
func (p *presence) excluding(names []string) *presence {
	if p == nil {
		return nil
	}
	result := &presence{record: p.record, entity: p.entity, snapshot: p.snapshot, forced: p.forced}
	if len(p.excluded) > 0 {
		result.excluded = append([]uint64(nil), p.excluded...)
	}
	for _, name := range names {
		if pos := p.record.position(name); pos >= 0 {
			setBit(&result.excluded, len(p.record.Fields), pos)
		}
	}
	return result
}

// Relation is one typed parent-to-child mutation edge.
type Relation struct {
	Field []int
	Child *Record
	Links []Link
}

// Link projects one parent key into its declared child foreign key.
type Link struct {
	Parent Field
	Child  Field
}

// Handler is the universal writer. Component-specific behavior is Metadata.
type Handler struct {
	inputType                 reflect.Type
	outputType                reflect.Type
	metadata                  *Metadata
	preBindingTransaction     bool
	transactionRetryMethod    int
	transactionRetrySupported bool
	recoveryMethod            int
	recoverySupported         bool
	scopedSequences           bool
}

func New(component *spec.Component, inputType, outputType reflect.Type, operation string) (*Handler, error) {
	metadata, err := Compile(component, inputType, outputType, operation)
	if err != nil {
		return nil, err
	}
	result := &Handler{inputType: inputType, outputType: outputType, metadata: metadata, preBindingTransaction: hasWritableRole(metadata.Root), scopedSequences: hasScopedSequences(metadata.Root)}
	if metadata.Root != nil && metadata.Root.HookType != nil {
		if method, ok := reflect.PointerTo(metadata.Root.HookType).MethodByName("Recover"); ok {
			result.recoveryMethod, result.recoverySupported = method.Index, true
		}
		if method, ok := reflect.PointerTo(metadata.Root.HookType).MethodByName("RetryTransaction"); ok {
			t := method.Type
			if t.IsVariadic() || t.NumIn() != 5 || t.In(1) != reflect.TypeFor[context.Context]() || t.In(2) != reflect.PointerTo(inputType) || t.In(3) != reflect.PointerTo(outputType) || t.In(4) != reflect.TypeFor[rhandler.MutationOutcome]() || t.NumOut() != 2 || t.Out(0) != reflect.TypeFor[bool]() || t.Out(1) != reflect.TypeFor[error]() {
				return nil, fmt.Errorf("RetryTransaction requires canonical Input, Output, MutationOutcome and (bool,error)")
			}
			result.transactionRetryMethod, result.transactionRetrySupported = method.Index, true
		}
	}
	return result, nil
}

func (h *Handler) InputType() reflect.Type  { return h.inputType }
func (h *Handler) OutputType() reflect.Type { return h.outputType }
func (*Handler) RequiresReadMetadata() bool { return true }

// Current and auxiliary evidence must be bound in the same owned transaction
// as the writes that consume it. Leaf auxiliary components have no database
// mutation and retain their non-transactional orchestration contract.
func (h *Handler) RequiresPreBindingTransaction() bool {
	return h != nil && h.preBindingTransaction
}

func hasWritableRole(record *Record) bool {
	if record == nil {
		return false
	}
	if !record.Auxiliary {
		return true
	}
	for _, relation := range record.Relations {
		if relation != nil && hasWritableRole(relation.Child) {
			return true
		}
	}
	return false
}

// Program is invocation-owned universal mutation state. The same type is used
// for every writer component; only Metadata and values differ.
type Program struct {
	finiteRootDecision          *finiteRootDecision
	sourceSliceGroups           map[*Action]*sourceSliceGroup
	reconciliation              *reconciliationAttempt
	phaseSelectionAttempted     bool
	rootPreparationAttempted    bool
	rootAllocationAttempted     bool
	rootProjectionAttempted     bool
	rootAdmissionAttempted      bool
	rootAdmissionPublished      bool
	rootAdmissionSpan           []*Action
	rootAdmissionContainer      *MutationActions
	reconciliationFrames        []*Frame
	reconciliationSeal          string
	queueSlots                  []queueSlotSeal
	afterValidateInputStarted   bool
	afterValidateInputViolation error
	afterQueueInputStarted      bool
	afterQueueInputSnapshot     string
	guardMu                     sync.Mutex
	guardIssued                 bool
	guardBinding                *drainowner.GuardBinding
	projectedRootActionsIssued  bool
	projectedRootActionCount    int
	guardBinder                 xhandler.Binder
	executionAttempted          bool
	executionFailure            error
	executionGuardRegistered    bool
	executionGuardReady         bool
	actionPolicyFacts           map[frameIdentity]*actionPolicyFacts
	scopedService               any
	metadata                    *Metadata
	input                       any
	output                      any
	original                    *OriginalInput
	database                    *DatabaseSnapshot
	frames                      *MutationFrames
	actions                     *MutationActions
	validation                  *FrameworkValidation
	hooks                       *Hooks
	hook                        reflect.Value
	hooksByRecord               map[*Record]reflect.Value
	stage                       Stage
	failed                      bool
	finalized                   bool
	componentHookAttempted      bool
	structuralError             error
	queueItems                  []*Action
	queueObserverPanic          bool
	queueInvocationID           uint64
	previousFields              map[*Record]fieldSet
	// graph caches insert lookups for the current frame topology.
	graph *graphIndex
	// typeFields caches the exported field set per Previous type.
	typeFields map[reflect.Type]fieldSet
}

func (p *Program) fieldsOf(typeOf reflect.Type) fieldSet {
	if p.typeFields == nil {
		p.typeFields = map[reflect.Type]fieldSet{}
	}
	if cached, ok := p.typeFields[typeOf]; ok {
		return cached
	}
	result := allFields(typeOf)
	p.typeFields[typeOf] = result
	return result
}

// graphIndex answers "which earlier insert produces this foreign-key value"
// in constant time. It is rebuilt whenever the frame list changes, replacing
// the previous frame-by-frame scans that made ordering and validation
// quadratic in the batch size.
type graphIndex struct {
	rows      []*Frame
	positions map[*Frame]int
	byPointer map[uintptr]*Frame
	inserts   map[insertKey][]insertReference
}

// positionHeap is a min-heap of frame positions for stable topological order.
type positionHeap []int

func (h positionHeap) Len() int           { return len(h) }
func (h positionHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h positionHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *positionHeap) Push(value any)    { *h = append(*h, value.(int)) }
func (h *positionHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

type insertReference struct {
	position int
	frame    *Frame
	value    reflect.Value
}

type insertKey struct {
	table, column string
	value         identityKey
}

func newInsertKey(table, column string, value reflect.Value) insertKey {
	return insertKey{table: strings.ToLower(table), column: strings.ToLower(column), value: scalarKey(indirect(value))}
}

// graphIndex returns the cached index for the current frames, rebuilding it
// when the frame slice was replaced or reordered.
func (p *Program) graphIndex() *graphIndex {
	rows := p.frames.Rows
	if p.graph != nil && len(p.graph.rows) == len(rows) && (len(rows) == 0 || &p.graph.rows[0] == &rows[0]) {
		return p.graph
	}
	index := &graphIndex{rows: rows, positions: make(map[*Frame]int, len(rows)), byPointer: make(map[uintptr]*Frame, len(rows)), inserts: map[insertKey][]insertReference{}}
	for position, frame := range rows {
		if frame == nil {
			continue
		}
		index.positions[frame] = position
		if frame.Entity.IsValid() && !frame.Entity.IsNil() {
			index.byPointer[frame.Entity.Pointer()] = frame
		}
		if frame.ExcludedWrite || frame.Action != xhandler.WriteInsert || frame.Record == nil || frame.Record.Auxiliary || !frame.Entity.IsValid() {
			continue
		}
		entity := frame.Entity.Elem()
		for _, field := range frame.Record.Fields {
			if field.Column == "" {
				continue
			}
			value := entity.FieldByIndex(field.Index)
			if !linkValueResolved(value) {
				continue
			}
			key := newInsertKey(frame.Record.Table, field.Column, value)
			index.inserts[key] = append(index.inserts[key], insertReference{position: position, frame: frame, value: value})
		}
	}
	p.graph = index
	return index
}

// producers returns the insert frames whose table/column value equals the
// supplied foreign-key value. The string key narrows candidates; linkedEqual
// keeps the exact equality semantics.
func (g *graphIndex) producers(field Field, value reflect.Value) []insertReference {
	if field.RefTable == "" || field.RefColumn == "" || !linkValueResolved(value) {
		return nil
	}
	candidates := g.inserts[newInsertKey(field.RefTable, field.RefColumn, value)]
	result := make([]insertReference, 0, len(candidates))
	for _, candidate := range candidates {
		if linkedEqual(value, candidate.value) {
			result = append(result, candidate)
		}
	}
	return result
}

type OriginalInput struct{ Presence map[uintptr]originalPresence }

// identityKey is an allocation-free rendering of a row identity or relation
// link value. Single integer keys fold into number; single strings use text;
// composite or exotic keys render to text.
type identityKey struct {
	kind   uint8
	number uint64
	text   string
}

const (
	identityInt uint8 = iota + 1
	identityUint
	identityString
	identityBool
	identityText
)

func (k identityKey) String() string {
	switch k.kind {
	case identityInt:
		return strconv.FormatInt(int64(k.number), 10)
	case identityUint:
		return strconv.FormatUint(k.number, 10)
	case identityBool:
		return strconv.FormatBool(k.number != 0)
	}
	return k.text
}

// scalarKey folds one dereferenced value into an identityKey component.
func scalarKey(value reflect.Value) identityKey {
	if !value.IsValid() {
		return identityKey{kind: identityText, text: "<nil>"}
	}
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return identityKey{kind: identityInt, number: uint64(value.Int())}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return identityKey{kind: identityUint, number: value.Uint()}
	case reflect.String:
		return identityKey{kind: identityString, text: value.String()}
	case reflect.Bool:
		if value.Bool() {
			return identityKey{kind: identityBool, number: 1}
		}
		return identityKey{kind: identityBool}
	}
	return identityKey{kind: identityText, text: fmt.Sprintf("%#v", value.Interface())}
}

// compositeKey joins several components; a single component is returned as is.
func compositeKey(parts []identityKey) identityKey {
	if len(parts) == 1 {
		return parts[0]
	}
	var builder strings.Builder
	for i, part := range parts {
		if i > 0 {
			builder.WriteByte(0)
		}
		builder.WriteByte(byte('0' + part.kind))
		builder.WriteString(part.String())
	}
	return identityKey{kind: identityText, text: builder.String()}
}

type rowIdentity struct {
	record *Record
	key    identityKey
}

// DatabaseSnapshot indexes Current rows by role and identity. ByRecord keeps
// the load order so assembled Previous relations are deterministic.
type DatabaseSnapshot struct {
	Rows     map[rowIdentity]reflect.Value
	ByRecord map[*Record][]reflect.Value
}
type MutationFrames struct{ Rows []*Frame }
type MutationActions struct{ Rows []*Action }
type FrameworkValidation struct{}
type Hooks struct{}
type Stage uint8

type Frame struct {
	framedEntity                 reflect.Value
	holderPosition               int
	holderIndexed, holderTracked bool
	Entity, Previous             reflect.Value
	ExpectedToken                reflect.Value
	Fields                       *presence
	ExcludedWrite                bool
	SkippedDelete                bool
	NoopMissingIdentity          bool
	Action                       xhandler.WriteAction
	Record                       *Record
	Location                     string
	Parent                       *Frame
	Original                     xhandler.OriginalPresence
	Hook                         reflect.Value
	ScopedAllocated              map[string]bool
}

type Action struct {
	Kind   xhandler.WriteAction
	Entity reflect.Value
	// Native actions retain their graph role even when entity pointers alias.
	frame *Frame
	// Minted only by native finite-phase lowering; never supplied by hooks.
	sourceGroup *sourceSliceGroup
	projected   *projectedRootAction
}

func (p *Program) allocate(ctx context.Context, sequencer xhandler.Sequencer, record *Record, roots reflect.Value) error {
	if !record.Auxiliary && record.WriterIdentityPolicy == assignedUpdateIdentity {
		selected := reflect.MakeSlice(reflect.SliceOf(reflect.PointerTo(record.EntityType)), 0, 0)
		for _, frame := range p.frames.Rows {
			if frame.Record == record && frame.Action == xhandler.WriteInsert && !frame.NoopMissingIdentity {
				selected = reflect.Append(selected, frame.Entity)
			}
		}
		if selected.Len() == 0 {
			return nil
		}
		roots = selected
	}

	// Auxiliary carriers retain SQL identities but never own allocation. Always
	// recurse below so physical descendants and same-table siblings remain owned.
	if !record.Auxiliary && record.Sequence != nil {
		if err := p.validateActionPolicyFacts(); err != nil {
			return err
		}
		if err := sequencer.Allocate(ctx, record.Table, roots.Interface(), record.Selector); err != nil {
			return fmt.Errorf("allocate %s: %w", record.Path, err)
		}
		for _, frame := range p.frames.Rows {
			if frame.Record == record {
				if err := p.advanceActionPolicyKey(frame, *record.Sequence); err != nil {
					return err
				}
			}
		}
		if err := p.validateActionPolicyFacts(); err != nil {
			return err
		}
	}
	// A transient child allocation performs an INSERT before rolling it back.
	// Supply newly allocated parent keys before that INSERT: disabling FK
	// checks in the allocator does not relax NOT NULL child columns.
	if len(record.Relations) > 0 {
		if err := p.reconcileLinks(false); err != nil {
			return err
		}
	}
	for _, relation := range record.Relations {
		if err := p.allocate(ctx, sequencer, relation.Child, roots); err != nil {
			return err
		}
	}
	return nil
}

func (p *Program) reconcileLinks(requireResolved bool) error {
	if err := p.validateActionPolicyFacts(); err != nil {
		return err
	}
	for _, frame := range p.frames.Rows {
		if frame == nil || frame.SkippedDelete || frame.Parent == nil || frame.Record == nil {
			continue
		}
		relation := relationFor(frame.Parent.Record, frame.Record)
		if relation == nil {
			return fmt.Errorf("writer relation metadata for %s is unavailable", frame.Record.Path)
		}
		for _, link := range relation.Links {
			parent := frame.Parent.Entity.Elem().FieldByIndex(link.Parent.Index)
			if !linkValueResolved(parent) {
				if requireResolved {
					return fmt.Errorf("writer relation %s parent link %s is unresolved", frame.Record.Path, link.Parent.Name)
				}
				continue
			}
			child := frame.Entity.Elem().FieldByIndex(link.Child.Index)
			if err := assignLinkedValue(child, parent); err != nil {
				return fmt.Errorf("writer relation %s link %s=%s: %w", frame.Record.Path, link.Parent.Name, link.Child.Name, err)
			}
			if err := p.advanceActionPolicyKey(frame, link.Child); err != nil {
				return err
			}
			// Updates stay sparse: a matched Previous row already carries this
			// foreign key (buildRecordFrames rejects parent-scope mismatches), so
			// only produced rows record the link as a supplied field.
			if frame.Previous.IsValid() && frame.Action != xhandler.WriteInsert {
				continue
			}
			markSupplied(frame.Entity.Elem(), link.Child)
			frame.Fields.force(link.Child.Name)
		}
	}
	return nil
}

func linkValueResolved(value reflect.Value) bool {
	if isNil(value) {
		return false
	}
	value = indirect(value)
	return value.IsValid() && !value.IsZero()
}

func relationFor(parent, child *Record) *Relation {
	if parent == nil {
		return nil
	}
	for _, relation := range parent.Relations {
		if relation != nil && relation.Child == child {
			return relation
		}
	}
	return nil
}

func assignLinkedValue(destination, source reflect.Value) error {
	if !destination.CanSet() {
		return fmt.Errorf("destination is not settable")
	}
	if source.Type().AssignableTo(destination.Type()) {
		destination.Set(source)
		return nil
	}
	if destination.Kind() == reflect.Pointer && source.Type().AssignableTo(destination.Type().Elem()) {
		value := reflect.New(destination.Type().Elem())
		value.Elem().Set(source)
		destination.Set(value)
		return nil
	}
	if source.Kind() == reflect.Pointer && !source.IsNil() && source.Elem().Type().AssignableTo(destination.Type()) {
		destination.Set(source.Elem())
		return nil
	}
	return fmt.Errorf("cannot assign %s to %s", source.Type(), destination.Type())
}

func markSupplied(entity reflect.Value, field Field) {
	if len(field.Has) == 0 {
		return
	}
	if field.marker != nil && entity.CanAddr() {
		structPtr := unsafe.Pointer(entity.UnsafeAddr())
		field.marker.EnsureHolder(structPtr)
		if err := field.marker.Set(structPtr, field.markerIndex, true); err == nil {
			return
		}
	}
	markerRoot := markerByIndex(entity, field.Has[:len(field.Has)-1])
	if !markerRoot.IsValid() || !markerRoot.CanSet() {
		return
	}
	if markerRoot.Kind() == reflect.Pointer && markerRoot.IsNil() {
		markerRoot.Set(reflect.New(markerRoot.Type().Elem()))
	}
	marker := markerByIndex(markerRoot, field.Has[len(field.Has)-1:])
	if marker.IsValid() && marker.CanSet() && marker.Kind() == reflect.Bool {
		marker.SetBool(true)
	}
}

func (h *Handler) NewPhaseObserver() xhandler.PhaseObserver {
	if h == nil || h.metadata == nil || h.metadata.Root == nil || h.metadata.Root.HookType == nil {
		return nil
	}
	hook := reflect.New(h.metadata.Root.HookType)
	observer, _ := hook.Interface().(xhandler.PhaseObserver)
	if observer == nil {
		if _, ok := hook.Interface().(xhandler.QueueAttemptObserver); ok {
			return &queuePhaseObserver{hook: hook}
		}
	}
	return observer
}
func (p *Program) usePhaseObserver(ctx context.Context) {
	scope := rhandler.PhaseScopeFromContext(ctx)
	if scope == nil || p.metadata.Root == nil || p.metadata.Root.HookType == nil {
		return
	}
	observer := scope.Observer()
	if wrapper, ok := observer.(*queuePhaseObserver); ok && wrapper.hook.Type() == reflect.PointerTo(p.metadata.Root.HookType) {
		p.hook = wrapper.hook
		p.hooksByRecord[p.metadata.Root] = p.hook
		return
	}
	if observer != nil && reflect.TypeOf(observer) == reflect.PointerTo(p.metadata.Root.HookType) {
		p.hook = reflect.ValueOf(observer)
		p.hooksByRecord[p.metadata.Root] = p.hook
	}
}
func (h *Handler) Execute(ctx context.Context, invocation rhandler.Invocation) (result any, err error) {
	scope := rhandler.PhaseScopeFromContext(ctx)
	// Keep policy capture before execution-begin observation when invoked without
	// the engine's ordinary CaptureInput snapshot. Policy-free behavior is unchanged.
	if h != nil && h.metadata != nil && hasRetainedWriterGuards(h.metadata.Root) && invocation.Snapshot == nil {
		program, captureErr := h.program(invocation.Input)
		if captureErr != nil {
			return nil, captureErr
		}
		invocation.Snapshot = program
	}
	if h != nil && h.metadata != nil && hasRetainedWriterGuards(h.metadata.Root) {
		program, guardErr := h.capturedProgram(invocation)
		if guardErr != nil {
			return nil, guardErr
		}
		defer func() {
			if value := recover(); value != nil {
				program.retainExecutionFailure(dexec.NewPanicError("captured writer execution", value))
				panic(value)
			}
			program.retainExecutionFailure(err)
		}()
		program.guardMu.Lock()
		issued, registered := program.guardIssued, program.executionGuardRegistered
		program.guardMu.Unlock()
		if issued && !registered {
			return nil, fmt.Errorf("captured writer guard registration did not succeed; fresh capture required")
		}
		if !issued {
			check, guardErr := h.CapturedExecutionGuard(invocation)
			if guardErr != nil {
				return nil, guardErr
			}
			service, guardErr := lookup[xhandler.DML](ctx, invocation.Binder, xhandler.DMLKey)
			if guardErr != nil {
				return nil, guardErr
			}
			registrar, supported := service.(interface {
				RegisterExecutionGuard(func(context.Context) error) error
				EnableCapturedExecutionGuards() error
				ValidateExecutionGuards(context.Context) error
				CloseMutationAdmission() error
			})
			if !supported {
				return nil, fmt.Errorf("captured writer execution requires a guarded DML journal")
			}
			if guardErr = registrar.EnableCapturedExecutionGuards(); guardErr != nil {
				return nil, guardErr
			}
			binding, bindingErr := h.CapturedExecutionGuardBinding(invocation)
			if bindingErr != nil {
				return nil, bindingErr
			}
			if bound, ok := service.(interface {
				RegisterBoundExecutionGuard(func(context.Context) error, *drainowner.GuardBinding) error
			}); ok && binding != nil {
				guardErr = bound.RegisterBoundExecutionGuard(check, binding)
			} else {
				guardErr = registrar.RegisterExecutionGuard(check)
			}
			if guardErr != nil {
				return nil, guardErr
			}
			if guardErr = h.CapturedExecutionGuardRegistered(invocation); guardErr != nil {
				return nil, guardErr
			}
		}
		if guardErr = program.admitCapturedExecution(invocation); guardErr != nil {
			return nil, guardErr
		}
	}
	err = scope.Run(ctx, xhandler.PhaseExecution, func() error { var failure error; result, failure = h.execute(ctx, invocation); return failure })
	// Buffered DML is completed by the engine after this boundary. An execution
	// end observer must not change the validated rows which the queue retains.
	if err == nil {
		if program, ok := invocation.Snapshot.(*Program); ok && program.metadata != nil && hasRetainedWriterGuards(program.metadata.Root) {
			err = program.validateActionPolicyFacts()
			if err == nil {
				err = program.validateActionPolicyActions()
				if err == nil {
					err = program.validateQueueSlots()
					if err == nil {
						err = program.validateAfterQueueInputState()
					}
				}
			}
			if err != nil {
				program.failed = true
			}
		}
	}
	return result, err
}
func (h *Handler) execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
	if h == nil || h.metadata == nil {
		return nil, fmt.Errorf("writer handler is not initialized")
	}
	program, _ := invocation.Snapshot.(*Program)
	var err error
	if program == nil {
		program, err = h.program(invocation.Input)
		if err != nil {
			return nil, err
		}
	}
	program.usePhaseObserver(ctx)
	if err = program.prepare(ctx, invocation.Binder); err != nil {
		return program.output, err
	}
	if handlerengine.IsImperativeComponent(ctx) && !program.explicitProtectedFlush() && (program.queueObserver() == nil || len(program.actions.Rows) > 0) {
		flusher, lookupErr := lookup[xhandler.Flusher](ctx, invocation.Binder, xhandler.FlusherKey)
		if lookupErr != nil {
			return program.output, lookupErr
		}
		if err = flusher.Flush(ctx, ""); err != nil {
			return program.output, fmt.Errorf("flush imperative writer: %w", err)
		}
	}
	return program.output, nil
}

// Configured writers keep their authored explicit boundaries; declaration
// alone never drains a table. Root completion owns the remaining journal.
func (p *Program) explicitProtectedFlush() bool {
	return p != nil && p.metadata != nil && p.metadata.Component != nil && p.metadata.Component.Settings != nil && len(p.metadata.Component.Settings.ProtectedFlushTables) != 0
}

func (h *Handler) CaptureInput(ctx context.Context, input any) (any, error) {
	program, err := h.program(input)
	if err != nil {
		return nil, err
	}
	program.usePhaseObserver(ctx)
	if err = prepareReadIndexes(ctx, input); err != nil {
		return nil, err
	}
	return program, nil
}

// CapturedExecutionGuard retains this exact native Program through deferred
// SQL execution. Registration does not arm unfinished action state.
func (h *Handler) capturedProgram(invocation rhandler.Invocation) (*Program, error) {
	program, ok := invocation.Snapshot.(*Program)
	if !ok || program == nil || program.metadata != h.metadata {
		return nil, fmt.Errorf("captured writer guard has invalid program ownership")
	}
	input, owner := reflect.ValueOf(invocation.Input), reflect.ValueOf(program.input)
	if !input.IsValid() || input.Kind() != reflect.Pointer || input.IsNil() || input.Type() != reflect.PointerTo(h.inputType) || !owner.IsValid() || owner.Type() != input.Type() || owner.Pointer() != input.Pointer() {
		return nil, fmt.Errorf("captured writer guard has invalid input ownership")
	}
	binding := reflect.ValueOf(invocation.Binder)
	if !binding.IsValid() || binding.Kind() != reflect.Pointer || binding.IsNil() {
		return nil, fmt.Errorf("captured writer guard requires definite invocation binder ownership")
	}
	return program, nil
}
func (p *Program) sameGuardBinder(invocation rhandler.Invocation) bool {
	binding := reflect.ValueOf(invocation.Binder)
	return binding.IsValid() && binding.Kind() == reflect.Pointer && !binding.IsNil() && p.guardBinder != nil && invocation.Binder == p.guardBinder
}
func (p *Program) retainExecutionFailure(err error) {
	if err == nil {
		return
	}
	p.guardMu.Lock()
	if p.executionFailure == nil {
		p.executionFailure = err
	}
	p.guardMu.Unlock()
}
func (p *Program) admitCapturedExecution(invocation rhandler.Invocation) error {
	p.guardMu.Lock()
	defer p.guardMu.Unlock()
	if !p.guardIssued || !p.executionGuardRegistered || !p.sameGuardBinder(invocation) {
		return fmt.Errorf("captured writer execution has invalid registration or binder ownership")
	}
	if p.executionAttempted || p.finalized {
		return fmt.Errorf("captured writer execution already attempted or finalized; fresh capture required")
	}
	p.executionAttempted = true
	return nil
}
func (h *Handler) CapturedExecutionGuard(invocation rhandler.Invocation) (func(context.Context) error, error) {
	if h == nil || h.metadata == nil || !hasRetainedWriterGuards(h.metadata.Root) {
		return nil, nil
	}
	program, err := h.capturedProgram(invocation)
	if err != nil {
		return nil, err
	}
	program.guardMu.Lock()
	if program.guardIssued || program.executionAttempted || program.finalized {
		program.guardMu.Unlock()
		return nil, fmt.Errorf("captured writer guard already issued or finalized; fresh capture required")
	}
	program.guardIssued, program.guardBinder = true, invocation.Binder
	program.guardBinding = drainowner.NewGuardBinding()
	program.guardMu.Unlock()
	return func(ctx context.Context) error {
		program.guardMu.Lock()
		ready, failure := program.executionGuardReady, program.executionFailure
		sourceAttempted, binding := program.phaseSelectionAttempted, program.guardBinding
		program.guardMu.Unlock()
		if failure != nil {
			return failure
		}
		if sourceAttempted {
			if !program.sameGuardBinder(invocation) {
				err := fmt.Errorf("source phase captured binder ownership changed")
				if program.reconciliation != nil {
					program.reconciliation.active = false
				}
				program.retainExecutionFailure(err)
				return err
			}
			terminal, scopeErr := drainowner.GuardPurpose(ctx, binding)
			if scopeErr == nil {
				scopeErr = ctx.Err()
			}
			if scopeErr == nil {
				scopeErr = program.validateFiniteRetainedActions()
			}
			if scopeErr == nil && terminal {
				scopeErr = fmt.Errorf("source phase execution is incomplete; completion proof unavailable")
			}
			if scopeErr != nil {
				if program.reconciliation != nil {
					program.reconciliation.active = false
				}
				program.retainExecutionFailure(scopeErr)
				return scopeErr
			}
		}
		if !ready {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := program.validateActionPolicyFacts(); err != nil {
			return err
		}
		if err := program.validateActionPolicyActions(); err != nil {
			return err
		}
		if err := program.validateQueueSlots(); err != nil {
			return err
		}
		return errors.Join(program.validateAfterQueueInputState(), program.validateReconciliationSeal())
	}, nil
}

// CapturedExecutionGuardBinding is an optional Datly-native registration
// association; ordinary external owners retain their existing guard fallback.
func (h *Handler) CapturedExecutionGuardBinding(invocation rhandler.Invocation) (*drainowner.GuardBinding, error) {
	program, err := h.capturedProgram(invocation)
	if err != nil {
		return nil, err
	}
	program.guardMu.Lock()
	defer program.guardMu.Unlock()
	if !program.guardIssued || program.executionAttempted || program.finalized || !program.sameGuardBinder(invocation) {
		return nil, fmt.Errorf("captured writer binding has invalid issuance or binder ownership")
	}
	if program.finiteRootDecision == nil {
		return nil, nil
	}
	return program.guardBinding, nil
}

func (h *Handler) CapturedExecutionGuardRegistered(invocation rhandler.Invocation) error {
	if h == nil || h.metadata == nil || !hasRetainedWriterGuards(h.metadata.Root) {
		return nil
	}
	program, err := h.capturedProgram(invocation)
	if err != nil {
		return err
	}
	program.guardMu.Lock()
	defer program.guardMu.Unlock()
	if !program.guardIssued || program.executionGuardRegistered || program.executionAttempted || program.finalized || !program.sameGuardBinder(invocation) {
		return fmt.Errorf("captured writer guard acknowledgment has invalid issuance or binder ownership")
	}
	program.executionGuardRegistered = true
	return nil
}

func prepareReadIndexes(ctx context.Context, input any) error {
	method := reflect.ValueOf(input).MethodByName("PrepareReadIndexes")
	if !method.IsValid() {
		return nil
	}
	results := method.Call([]reflect.Value{reflect.ValueOf(ctx)})
	return methodError("PrepareReadIndexes", results)
}

func (*Handler) EarlyErrorOutputEnabled() bool { return true }

func (h *Handler) CapturedErrorOutput(_ context.Context, invocation rhandler.Invocation, cause error) (any, error) {
	program, ok := invocation.Snapshot.(*Program)
	if h == nil || h.metadata == nil || !ok || program == nil || program.metadata != h.metadata {
		return nil, fmt.Errorf("captured writer output has invalid program ownership")
	}
	input := reflect.ValueOf(invocation.Input)
	owner := reflect.ValueOf(program.input)
	if !input.IsValid() || input.Type() != reflect.PointerTo(h.inputType) || input.IsNil() || !owner.IsValid() || owner.Type() != input.Type() || owner.Pointer() != input.Pointer() {
		return nil, fmt.Errorf("captured writer output has invalid input ownership")
	}
	output := reflect.ValueOf(program.output)
	if !output.IsValid() || output.Type() != reflect.PointerTo(h.outputType) || output.IsNil() {
		return nil, fmt.Errorf("captured writer output has invalid canonical type")
	}
	var body xresponse.BodyError
	if !errors.As(cause, &body) {
		return nil, nil
	}
	public, ok := body.(*xresponse.Error)
	if !ok || public == nil {
		return nil, nil
	}
	payload := reflect.ValueOf(public.Payload)
	if !payload.IsValid() || payload.Type() != output.Type() || payload.IsNil() {
		return nil, nil
	}
	if payload.Pointer() != output.Pointer() {
		output.Elem().Set(payload.Elem())
	}
	return program.output, nil
}

func (h *Handler) FinalizeOutcome(ctx context.Context, invocation rhandler.Invocation, result any, outcome xhandler.Outcome) error {
	program, _ := invocation.Snapshot.(*Program)
	if program == nil {
		if outcome.Error != nil {
			if finalizer, ok := result.(xhandler.ErrorFinalizer); ok {
				return finalizer.Finalize(ctx, outcome.Error)
			}
		}
		return nil
	}
	guarded := program.metadata != nil && hasRetainedWriterGuards(program.metadata.Root)
	if guarded {
		program.guardMu.Lock()
		alreadyFinalized := program.finalized
		program.finalized = true
		program.guardMu.Unlock()
		if alreadyFinalized {
			return nil
		}
	}
	if !program.hook.IsValid() {
		return nil
	}
	if program.componentHook() && !guarded {
		if program.finalized {
			return nil
		}
		program.finalized = true
	}
	method := program.hook.MethodByName("Finalize")
	if !method.IsValid() {
		return nil
	}
	results := method.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(program.input), reflect.ValueOf(program.output), reflect.ValueOf(outcome)})
	return methodError("Finalize", results)
}

func (h *Handler) program(input any) (*Program, error) {
	value := reflect.ValueOf(input)
	if !value.IsValid() || value.Type() != reflect.PointerTo(h.inputType) || value.IsNil() {
		return nil, fmt.Errorf("writer input must be *%s, got %T", h.inputType, input)
	}
	output := reflect.New(h.outputType)
	result := &Program{
		metadata: h.metadata, input: input, output: output.Interface(), original: &OriginalInput{Presence: map[uintptr]originalPresence{}},
		database: &DatabaseSnapshot{Rows: map[rowIdentity]reflect.Value{}, ByRecord: map[*Record][]reflect.Value{}}, frames: &MutationFrames{},
		actions: &MutationActions{}, validation: &FrameworkValidation{}, hooks: &Hooks{}, hooksByRecord: map[*Record]reflect.Value{}, failed: true,
	}
	if h.metadata.Root != nil {
		result.prepareHooks(h.metadata.Root)
		result.hook = result.hooksByRecord[h.metadata.Root]
		entities := value.Elem().Field(h.metadata.InputField)
		if err := result.captureFiniteRootDecision(entities); err != nil {
			return nil, err
		}
		if err := result.captureOriginal(h.metadata.Root, entities); err != nil {
			return nil, err
		}
	} else if h.metadata.HookType != nil {
		result.hook = reflect.New(h.metadata.HookType)
	}
	return result, nil
}

func (p *Program) captureOriginal(record *Record, rows reflect.Value) error {
	return p.captureOriginalAt(record, rows, nil)
}

func (p *Program) captureOriginalAt(record *Record, rows reflect.Value, parent *Frame) error {
	indexed := rows.Kind() == reflect.Slice
	if rows.Kind() == reflect.Pointer {
		if rows.IsNil() {
			return nil
		}
		wrapped := reflect.MakeSlice(reflect.SliceOf(rows.Type()), 1, 1)
		wrapped.Index(0).Set(rows)
		rows = wrapped
	}
	if rows.Kind() != reflect.Slice {
		return fmt.Errorf("writer role %s requires a record or collection, got %s", record.Path, rows.Type())
	}
	for i := 0; i < rows.Len(); i++ {
		entity := rows.Index(i)
		if entity.IsNil() {
			if p.skipAuxiliaryNull(record, indexed) || p.deferRootNull(record, i, indexed) || p.deferNestedNull(record, p.rowLocation(record, parent, i, indexed), indexed) {
				continue
			}
			return fmt.Errorf("writer row %d is nil", i)
		}
		p.captureEntityOriginal(record, entity)
		location := &Frame{Record: record, Entity: reflect.ValueOf(entity.Interface()), framedEntity: reflect.ValueOf(entity.Interface()), holderPosition: i, holderIndexed: indexed, holderTracked: true, Parent: parent, Location: p.rowLocation(record, parent, i, indexed)}
		p.captureActionPolicyOwner(location)
		for _, relation := range record.Relations {
			if err := p.captureOriginalAt(relation.Child, entity.Elem().FieldByIndex(relation.Field), location); err != nil {
				return err
			}
		}
	}
	return nil
}

// captureEntityOriginal snapshots one row's presence markers and concurrency
// token exactly once. Rows a lifecycle appends during Init (for example
// explicit deletions derived from Previous) are captured when first framed, so
// their authored token remains available to the concurrency check.
func (p *Program) captureEntityOriginal(record *Record, entity reflect.Value) originalPresence {
	p.captureActionPolicyFacts(record, entity)
	if existing, ok := p.original.Presence[entity.Pointer()]; ok {
		return existing
	}
	captured := originalPresence{presence: snapshotPresence(record, entity.Elem()), available: presenceAvailable(entity.Elem())}
	if hasReconciliation(p.metadata.Root) {
		captured.identityValues = map[string]reflect.Value{}
		for _, key := range record.Keys {
			captured.identityValues[key.Name] = cloneTokenValue(entity.Elem().FieldByIndex(key.Index))
		}
	}
	for _, plan := range record.ScopedSequences {
		value := entity.Elem().FieldByIndex(plan.Field.Index)
		if plan.AllocateNull && value.Kind() == reflect.Pointer && value.IsNil() {
			if captured.scopedNull == nil {
				captured.scopedNull = map[string]bool{}
			}
			captured.scopedNull[plan.Field.Name] = true
		}
	}
	if record.ConcurrencyToken != nil {
		captured.token = cloneTokenValue(entity.Elem().FieldByIndex(record.ConcurrencyToken.Index))
	}
	p.original.Presence[entity.Pointer()] = captured
	return captured
}

func (p *Program) prepareHooks(record *Record) {
	if record == nil {
		return
	}
	if record.HookType != nil {
		p.hooksByRecord[record] = reflect.New(record.HookType)
	}
	for _, relation := range record.Relations {
		p.prepareHooks(relation.Child)
	}
}

func (p *Program) prepare(ctx context.Context, binder xhandler.Binder) error {
	if p.afterValidateInputStarted {
		return fmt.Errorf("AfterValidateInput execution already attempted; fresh capture required")
	}
	if p.afterQueueInputWasStarted() {
		return fmt.Errorf("AfterQueueInput execution already attempted; fresh capture required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.componentHook() {
		if p.componentHookAttempted {
			return fmt.Errorf("auxiliary component lifecycle already attempted")
		}
		p.componentHookAttempted = true
		if binder == nil {
			return fmt.Errorf("auxiliary component lifecycle requires a binder")
		}
		if err := binder.Bind(ctx, p.hook.Interface()); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		entities := reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField)
		reflect.ValueOf(p.output).Elem().Field(p.metadata.OutputField).Set(entities)
		frame := &Frame{Entity: reflect.ValueOf(p.input), Hook: p.hook, Record: p.metadata.Root}
		if err := p.callEntityHook(ctx, "Init", frame); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := p.callEntityHook(ctx, "Validate", frame); err != nil {
			return err
		}
		p.failed = false
		return nil
	}
	phases := rhandler.PhaseScopeFromContext(ctx)
	input := reflect.ValueOf(p.input).Elem()
	entities := input.Field(p.metadata.InputField)
	if p.metadata.Root == nil {
		return fmt.Errorf("writer root metadata is unavailable")
	}
	if err := p.indexRecordCurrent(input, p.metadata.Root); err != nil {
		return err
	}
	if err := p.assemblePreviousRelations(p.metadata.Root); err != nil {
		return err
	}
	if err := p.validateFiniteRootDecision(entities); err != nil {
		return err
	}
	if err := p.buildRecordFrames(ctx, binder, p.metadata.Root, entities, nil); err != nil {
		return err
	}
	for record, hook := range p.hooksByRecord {
		if !hook.IsValid() {
			continue
		}
		if binder == nil {
			return fmt.Errorf("writer hooks for %s require a binder", record.Path)
		}
		if err := binder.Bind(ctx, hook.Interface()); err != nil {
			return fmt.Errorf("bind writer hooks for %s: %w", record.Path, err)
		}
	}
	validator, err := lookup[xhandler.Validator](ctx, binder, xhandler.FrameworkValidatorKey)
	if err != nil {
		return err
	}
	if err = p.reconcileLinks(false); err != nil {
		return err
	}
	for _, frame := range p.frames.Rows {
		if err = p.applyInvariants(frame); err != nil {
			return err
		}
		if err = p.checkConcurrency(frame); err != nil {
			return err
		}
	}
	if err = phases.Run(ctx, xhandler.PhaseInitialization, func() error {
		initialized := map[frameIdentity]bool{}
		for _, frame := range p.frames.Rows {
			if err = p.callEntityHook(ctx, "Init", frame); err != nil {
				return err
			}
			initialized[identityOfFrame(frame)] = true
			// Init is the supported phase for marker-aware business defaults and
			// sparse server-owned transitions. Frame presence is a live view of the
			// entity marker, so setter changes are visible to validation and action
			// selection without a synchronisation pass; invariant backfill stays in
			// the writer overlay and never mutates client markers.
		}
		// A lifecycle may implement atomic replacement by appending explicit
		// deletion rows derived from the assembled Previous graph. Rebuild frames
		// once after Init so those new rows participate in validation, ordering and
		// DML without invoking Init twice for the original topology.
		// Validate surviving first-pass identities before the rebuild can recapture them.
		// Init-cleared auxiliary subtrees remain eligible for bounded pruning.
		if err = p.discardInitializedAuxiliaryNullFrames(); err != nil {
			return err
		}
		p.frames = &MutationFrames{}
		if err = p.validateFiniteRootDecision(entities); err != nil {
			return err
		}
		if err = p.buildRecordFrames(ctx, binder, p.metadata.Root, entities, nil); err != nil {
			return err
		}
		if err = p.reconcileLinks(false); err != nil {
			return err
		}
		for _, frame := range p.frames.Rows {
			if p.skippedAuxiliaryAncestor(frame) != nil {
				continue
			}
			if err = p.validateAuxiliaryFrameIdentity(frame); err != nil {
				return err
			}
			if err = p.applyInvariants(frame); err != nil {
				return err
			}
			if err = p.checkConcurrency(frame); err != nil {
				return err
			}
			if !initialized[identityOfFrame(frame)] {
				if err = p.callEntityHook(ctx, "Init", frame); err != nil {
					return err
				}
			}
		}
		if err = p.discardInitializedAuxiliaryNullFrames(); err != nil {
			return err
		}
		if err = p.orderFramesByReferences(); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}
	if err = phases.Run(ctx, xhandler.PhaseValidation, func() error {
		if p.structuralError != nil {
			return p.structuralError
		}
		initialError := p.validateFrames(ctx, validator, false)
		if p.aggregateValidation() {
			var schemaFailure *initialSchemaViolations
			if initialError != nil && !errors.As(initialError, &schemaFailure) {
				return initialError
			}
			report := &inputValidationReport{}
			if schemaFailure != nil {
				report.native = schemaFailure.validation
			}
			if err = ctx.Err(); err != nil {
				return err
			}
			results := p.hook.MethodByName("ValidateInput").Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(p.input), reflect.ValueOf(p.output), reflect.ValueOf(report)})
			if err = methodError("ValidateInput", results); err != nil {
				return err
			}
			if err = ctx.Err(); err != nil {
				return err
			}
			if err = report.result().Err(); err != nil {
				return rhandler.ValidationPhaseFailure(err)
			}
		} else if initialError != nil {
			return initialError
		}
		for _, frame := range p.frames.Rows {
			if !frame.Record.Auxiliary && frame.Action == xhandler.WriteInsert {
				if err = validateInsertIdentity(frame.Record, frame.Entity.Elem(), frame.Parent); err != nil {
					return err
				}
			}
			if err = p.callEntityHook(ctx, "Validate", frame); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return err
	}
	if err = p.callAfterValidateInput(ctx, binder, validator); err != nil {
		return err
	}
	for _, frame := range p.frames.Rows {
		if frame.Record.Auxiliary || frame.SkippedDelete || frame.NoopMissingIdentity {
			continue
		}
		if frame.Action == xhandler.WriteUpdate && !hasMutableFields(frame) {
			if p.queueObserver() != nil && frame.Previous.IsValid() {
				p.queueItems = append(p.queueItems, &Action{Kind: frame.Action, Entity: frame.Entity, frame: frame})
			}
			continue
		}
		action := &Action{Kind: frame.Action, Entity: frame.Entity, frame: frame}
		if frame.Action == xhandler.WriteDelete {
			p.actions.Rows = append([]*Action{action}, p.actions.Rows...)
			if p.queueObserver() != nil {
				p.queueItems = append([]*Action{action}, p.queueItems...)
			}
		} else {
			p.actions.Rows = append(p.actions.Rows, action)
			if p.queueObserver() != nil {
				p.queueItems = append(p.queueItems, action)
			}
		}
	}

	if err = p.validateAuxiliaryTopology(); err != nil {
		return err
	}
	if err = p.preflightQueueContracts(ctx, binder); err != nil {
		return err
	}
	if len(p.actions.Rows) > 0 {
		starter, lookupErr := lookup[xhandler.TransactionStarter](ctx, binder, xhandler.TransactionStarterKey)
		if lookupErr != nil {
			return lookupErr
		}
		if err = starter.Start(ctx); err != nil {
			return err
		}
	}
	if err = p.captureReconciliationAllocation(ctx); err != nil {
		return err
	}
	if hasAction(p.actions.Rows, xhandler.WriteInsert) {
		if err = phases.Run(ctx, xhandler.PhaseAllocation, func() error {
			sequencer, lookupErr := lookup[xhandler.Sequencer](ctx, binder, xhandler.SequencerKey)
			if lookupErr != nil {
				return lookupErr
			}
			if err = p.allocate(ctx, sequencer, p.metadata.Root, entities); err != nil {
				return err
			}
			if err = p.reconcileLinks(false); err != nil {
				return err
			}
			if err = p.allocateScoped(ctx, sequencer); err != nil {
				return err
			}

			return nil
		}); err != nil {
			return err
		}

	}
	var sequenceSnapshot string
	if hasReconciliation(p.metadata.Root) {
		sequenceSnapshot, err = p.reconciliationState()
		if err != nil {
			return err
		}
	}
	for _, frame := range p.frames.Rows {
		if err = p.callEntityHook(ctx, "AfterSequence", frame); err != nil {
			return err
		}
	}
	if hasReconciliation(p.metadata.Root) {
		after, e := p.reconciliationState()
		if e != nil {
			return e
		}
		if after != sequenceSnapshot {
			return fmt.Errorf("AfterSequence changed finite_reconciliation allocated graph or evidence")
		}
	}
	if err = p.validateAuxiliaryTopology(); err != nil {
		return err
	}
	if err = p.reconcileLinks(true); err != nil {
		return err
	}
	if err = p.reconcileInput(ctx); err != nil {
		return err
	}
	if err = p.evaluateWriteEligibility(ctx); err != nil {
		return err
	}
	if hasReconciliation(p.metadata.Root) {
		if err = ctx.Err(); err != nil {
			return err
		}
	}
	if err = p.validateFrames(ctx, validator, true); err != nil {
		return err
	}
	if hasReconciliation(p.metadata.Root) {
		if err = ctx.Err(); err != nil {
			return err
		}
	}
	p.filterIneligibleActions()
	if err = p.sealReconciliation(); err != nil {
		return err
	}
	p.freezeActionPolicyParticipants()
	p.guardMu.Lock()
	p.executionGuardReady = true
	p.guardMu.Unlock()
	if err = p.validateActionPolicyActions(); err != nil {
		return err
	}
	if err = phases.Run(ctx, xhandler.PhaseQueue, func() error {
		return p.queue(ctx, binder)
	}); err != nil {
		return err
	}

	if err = p.validateAuxiliaryTopology(); err != nil {
		return err
	}
	if err = p.validateActionPolicyFacts(); err != nil {
		return err
	}
	if err = p.callAfterQueueInput(ctx, binder); err != nil {
		return err
	}
	output := reflect.ValueOf(p.output).Elem()
	output.Field(p.metadata.OutputField).Set(entities)
	if status := output.FieldByName("Status"); status.IsValid() && status.CanSet() && status.Kind() == reflect.String && status.String() == "" {
		status.SetString("ok")
	} else if status.IsValid() && status.Kind() == reflect.Struct {
		if value := status.FieldByName("Status"); value.IsValid() && value.CanSet() && value.Kind() == reflect.String && value.String() == "" {
			value.SetString("ok")
		}
	}
	p.failed = false
	return nil
}

func (p *Program) queuePhysical(ctx context.Context, binder xhandler.Binder, dml xhandler.DML, action *Action, frame *Frame, queued *bool) error {
	if err := p.validateActionPolicyFacts(); err != nil {
		return err
	}
	if err := p.validateAuxiliaryFrameIdentity(frame); err != nil {
		return err
	}
	if invalid := p.skippedAuxiliaryAncestor(frame); invalid != nil {
		return fmt.Errorf("writer row at %s is nil", invalid.Location)
	}
	var err error
	value := action.Entity.Interface()
	table := frame.Record.Table
	var options []xhandler.Option
	if token := frame.Record.ConcurrencyToken; token != nil && (action.Kind == xhandler.WriteUpdate || action.Kind == xhandler.WriteDelete) {
		persisted := frame.Previous
		if persisted.IsValid() && persisted.Kind() == reflect.Pointer {
			persisted = persisted.Elem()
		}
		if !frame.ExpectedToken.IsValid() || !persisted.IsValid() || persisted.Kind() != reflect.Struct {
			return &xhandler.Conflict{Entity: frame.Record.Path, Field: token.Name, Reason: "persisted concurrency token is unavailable"}
		}
		previousToken := previousField(persisted, frame.Record, token)
		if !previousToken.IsValid() {
			return &xhandler.Conflict{Entity: frame.Record.Path, Field: token.Name, Reason: "persisted concurrency token is unavailable"}
		}
		options = append(options, xhandler.WithIfMatch(token.Column, previousToken.Interface()))
	}
	var criteria *sqlx.Criteria
	if frame.Record.MutationPredicateGroup != nil && (action.Kind == xhandler.WriteUpdate || action.Kind == xhandler.WriteDelete) {
		predicateCtx, dialectErr := mutationPredicateContext(ctx, binder)
		if dialectErr != nil {
			return fmt.Errorf("mutation predicate %s dialect: %w", frame.Record.Path, dialectErr)
		}
		criteria, err = p.metadata.Predicates.Criteria(predicateCtx, binder, *frame.Record.MutationPredicateGroup)
		if err != nil {
			return fmt.Errorf("mutation predicate %s: %w", frame.Record.Path, err)
		}
	}
	switch action.Kind {
	case xhandler.WriteInsert:
		if frame.Record.QueueContract != "" {
			err = p.admitSourceRow(dml, action, frame)
		} else {
			err = dml.Insert(table, value)
		}
	case xhandler.WriteUpdate, xhandler.WriteDelete:
		if criteria != nil {
			native, ok := dml.(rhandler.CriteriaDML)
			if !ok {
				return fmt.Errorf("mutation predicate requires native CriteriaDML")
			}
			if action.Kind == xhandler.WriteUpdate {
				err = native.UpdateWithCriteria(table, value, criteria, options...)
			} else {
				err = native.DeleteWithCriteria(table, value, criteria, options...)
			}
		} else if len(options) != 0 {
			native, ok := dml.(xhandler.MatchedDML)
			if !ok {
				return &xhandler.Conflict{Entity: frame.Record.Path, Reason: "atomic matched DML is unavailable"}
			}
			if action.Kind == xhandler.WriteUpdate {
				err = native.UpdateWithOptions(table, value, options...)
			} else {
				err = native.DeleteWithOptions(table, value, options...)
			}
		} else if action.Kind == xhandler.WriteUpdate {
			err = dml.Update(table, value)
		} else {
			if frame.Record.QueueContract != "" {
				err = p.admitSourceRow(dml, action, frame)
			} else {
				err = dml.Delete(table, value)
			}
		}
	default:
		err = fmt.Errorf("unsupported writer action %q", action.Kind)
	}
	if err != nil {
		return fmt.Errorf("%s %s: %w", action.Kind, table, err)
	}
	if queued != nil {
		*queued = true
	}
	if err = p.callEntityHook(ctx, "AfterQueue", frame); err != nil {
		return err
	}
	return p.validateQueuedContractState(ctx, binder)
}

func (p *Program) componentHook() bool {
	return p != nil && p.metadata != nil && p.metadata.Root != nil && p.metadata.Root.Auxiliary && len(p.metadata.Root.Relations) == 0 && p.hook.IsValid()
}

type frameIdentity struct {
	record  *Record
	pointer uintptr
}

func identityOfFrame(frame *Frame) frameIdentity {
	if frame == nil || !frame.Entity.IsValid() || frame.Entity.IsNil() {
		return frameIdentity{}
	}
	return frameIdentity{record: frame.Record, pointer: frame.Entity.Pointer()}
}

// actionFrame retains the native action's authoritative role, never a
// last-wins entity-pointer lookup. An uncaptured association fails closed.
func (p *Program) actionFrame(action *Action) *Frame {
	if action != nil && action.projected == nil && p.reconciliation != nil {
		for _, owned := range p.reconciliation.projectedActions {
			if owned == action {
				_ = p.validateProjectedRootAction(action)
				return nil
			}
		}
	}
	if action != nil && action.projected != nil {
		if err := p.validateProjectedRootAction(action); err != nil {
			return nil
		}
	}
	if action == nil || action.frame == nil || !action.Entity.IsValid() || action.Entity.Kind() != reflect.Pointer || action.Entity.IsNil() {
		return nil
	}
	frame := action.frame
	if !frame.Entity.IsValid() || frame.Entity.Kind() != reflect.Pointer || frame.Entity.IsNil() || (frame.Entity.Pointer() != action.Entity.Pointer() && action.projected == nil) {
		return nil
	}
	for _, owned := range p.frames.Rows {
		if owned == frame {
			return frame
		}
	}
	return nil
}

func (p *Program) frameFor(entity reflect.Value) *Frame {
	if !entity.IsValid() || entity.IsNil() {
		return nil
	}
	return p.graphIndex().byPointer[entity.Pointer()]
}

func hasMutableFields(frame *Frame) bool {
	if frame == nil || frame.Record == nil {
		return false
	}
	// Identity fields are match criteria, never changes. A supplied concurrency
	// token that still equals the expected (Previous) value is also criteria; a
	// token a lifecycle advanced during Init is a real change and is written.
	criteria := map[string]bool{}
	for _, key := range frame.Record.Keys {
		criteria[key.Name] = true
	}
	if token := frame.Record.ConcurrencyToken; token != nil && frame.Entity.IsValid() && frame.ExpectedToken.IsValid() {
		current := frame.Entity.Elem().FieldByIndex(token.Index)
		if concurrencyTokenEqual(frame.ExpectedToken.Interface(), current.Interface()) {
			criteria[token.Name] = true
		}
	}
	for pos, field := range frame.Record.Fields {
		if !criteria[field.Name] && frame.Fields.hasPos(pos) {
			return true
		}
	}
	return false
}

func (p *Program) unresolvedParentLinks(frame *Frame) bool {
	if frame == nil || frame.Parent == nil {
		return false
	}
	relation := relationFor(frame.Parent.Record, frame.Record)
	if relation == nil {
		return true
	}
	for _, link := range relation.Links {
		if !linkValueResolved(frame.Parent.Entity.Elem().FieldByIndex(link.Parent.Index)) {
			return true
		}
	}
	return false
}

func (p *Program) validationOptions(frame *Frame, transactionStarted bool) xhandler.ValidationOptions {
	unique, refs := true, true
	if frame.NoopMissingIdentity {
		// An unmatched no-op has no Previous evidence. Complete candidate
		// checks use the existing no-Previous validation contract; this does
		// not classify the row as an insert or authorize persistence.
		options := xhandler.ValidationOptions{Action: xhandler.WriteInsert, Shallow: true, Location: frame.Location, CheckUnique: &unique, CheckRef: &refs}
		if frame.Record.InsertValidationPresence && !transactionStarted {
			options.HonorPresence, options.Fields = true, frame.Fields
		}
		return options
	}

	options := xhandler.ValidationOptions{Action: frame.Action, Location: frame.Location, Shallow: true, CheckUnique: &unique, CheckRef: &refs}
	if transactionStarted && frame.Action == xhandler.WriteDelete && hasReconciliation(p.metadata.Root) {
		options.Action = xhandler.WriteUpdate
	}
	if frame.Action == xhandler.WriteInsert && frame.Record.InsertValidationPresence && !transactionStarted {
		options.HonorPresence = true
		options.Fields = frame.Fields
	}
	if frame.Previous.IsValid() && !isPolicyInsert(frame) && !p.isFiniteRootInsert(frame) {
		options.Previous = frame.Previous.Interface()
		options.PreviousFields = p.fieldsOf(frame.Previous.Elem().Type())
		options.Fields = frame.Fields
	}
	graphReferences := p.satisfiedGraphReferences(frame)
	if frame.Action == xhandler.WriteUpdate && len(graphReferences) > 0 {
		// SQLX reference receipts are insert-only. For a sparse update whose new
		// FK value is proven to match an earlier insert in this ordered graph,
		// exclude only that reference field from the external database lookup.
		// The generated transaction and database FK still enforce the value.
		excluded := make([]string, 0, len(graphReferences))
		for _, reference := range graphReferences {
			excluded = append(excluded, reference.Field)
		}
		options.Fields = frame.Fields.excluding(excluded)
	}
	if frame.Action == xhandler.WriteInsert && frame.Parent != nil {
		physicalParentInsert := frame.Parent.Record != nil && !frame.Parent.Record.Auxiliary && !frame.Parent.ExcludedWrite && frame.Parent.Action == xhandler.WriteInsert
		if relation := relationFor(frame.Parent.Record, frame.Record); relation != nil {
			deferred := fieldSet{}
			for _, link := range relation.Links {
				if !transactionStarted && physicalParentInsert || !frame.Parent.Record.Auxiliary && !linkValueResolved(frame.Parent.Entity.Elem().FieldByIndex(link.Parent.Index)) {
					deferred[link.Child.Name] = true
					continue
				}
				if transactionStarted && physicalParentInsert && strings.EqualFold(link.Child.RefTable, frame.Parent.Record.Table) && strings.EqualFold(link.Child.RefColumn, link.Parent.Column) {
					options.SatisfiedReferences = append(options.SatisfiedReferences, xhandler.ValidationReference{Field: link.Child.Name, Schema: link.Child.RefDB, Table: link.Child.RefTable, Column: link.Child.RefColumn})
				}
			}
			if len(deferred) > 0 {
				options.DeferredFields = deferred
			}
		}
	}
	if frame.Action == xhandler.WriteInsert {
		for _, reference := range graphReferences {
			if !transactionStarted {
				if options.DeferredFields == nil {
					options.DeferredFields = fieldSet{}
				}
				options.DeferredFields.(fieldSet)[reference.Field] = true
				continue
			}
			duplicate := false
			for _, existing := range options.SatisfiedReferences {
				if existing.Field == reference.Field && strings.EqualFold(existing.Schema, reference.Schema) && strings.EqualFold(existing.Table, reference.Table) && strings.EqualFold(existing.Column, reference.Column) {
					duplicate = true
					break
				}
			}
			if !duplicate {
				options.SatisfiedReferences = append(options.SatisfiedReferences, reference)
			}
		}
	}
	return options
}

func (p *Program) satisfiedGraphReferences(frame *Frame) []xhandler.ValidationReference {
	if p == nil || frame == nil || frame.Record == nil || !frame.Entity.IsValid() {
		return nil
	}
	index := p.graphIndex()
	position, ok := index.positions[frame]
	if !ok {
		return nil
	}
	current := frame.Entity.Elem()
	var result []xhandler.ValidationReference
	for _, field := range frame.Record.Fields {
		if field.RefTable == "" || field.RefColumn == "" {
			continue
		}
		for _, producer := range index.producers(field, current.FieldByIndex(field.Index)) {
			if producer.position < position && producer.frame != frame {
				result = append(result, xhandler.ValidationReference{Field: field.Name, Schema: field.RefDB, Table: field.RefTable, Column: field.RefColumn})
			}
		}
	}
	return result
}

// orderFramesByReferences keeps a graph's inserts ahead of rows that refer to
// them, including references between different root relations. Generated view
// order alone cannot express every foreign-key dependency.
func (p *Program) orderFramesByReferences() error {
	if p == nil || p.frames == nil || len(p.frames.Rows) < 2 {
		return nil
	}
	rows := p.frames.Rows
	index := p.graphIndex()
	dependencies := make([][]int, len(rows))
	for i, frame := range rows {
		if frame == nil || frame.Record == nil || !frame.Entity.IsValid() {
			continue
		}
		if frame.Parent != nil {
			if parentIndex, ok := index.positions[frame.Parent]; ok && parentIndex != i {
				dependencies[i] = append(dependencies[i], parentIndex)
			}
		}
		if frame.Action == xhandler.WriteDelete {
			continue
		}
		current := frame.Entity.Elem()
		for _, field := range frame.Record.Fields {
			if field.RefTable == "" || field.RefColumn == "" {
				continue
			}
			for _, producer := range index.producers(field, current.FieldByIndex(field.Index)) {
				if producer.position != i {
					dependencies[i] = append(dependencies[i], producer.position)
				}
			}
		}
	}
	// Kahn's algorithm with a position-ordered ready queue keeps the original
	// relative order for frames that have no dependency between them.
	pending := make([]int, len(rows))
	dependents := make([][]int, len(rows))
	for i, deps := range dependencies {
		pending[i] = len(deps)
		for _, dependency := range deps {
			dependents[dependency] = append(dependents[dependency], i)
		}
	}
	ready := &positionHeap{}
	for i := range rows {
		if pending[i] == 0 {
			heap.Push(ready, i)
		}
	}
	ordered := make([]*Frame, 0, len(rows))
	for ready.Len() > 0 {
		selected := heap.Pop(ready).(int)
		ordered = append(ordered, rows[selected])
		for _, dependent := range dependents[selected] {
			pending[dependent]--
			if pending[dependent] == 0 {
				heap.Push(ready, dependent)
			}
		}
	}
	if len(ordered) != len(rows) {
		return fmt.Errorf("writer graph has cyclic insert references")
	}
	p.frames.Rows = ordered
	p.graph = nil
	return nil
}

func (p *Program) aggregateValidation() bool {
	return p.hook.IsValid() && p.hook.MethodByName("ValidateInput").IsValid()
}

func (p *Program) validateFrames(ctx context.Context, validator xhandler.Validator, transactionStarted bool) error {
	return p.validateFrameSubset(ctx, validator, transactionStarted, p.frames.Rows)
}

// validateFrameSubset retains the canonical graph for reference evidence while
// validating only the supplied native frames. It neither changes frame topology
// nor initializes, allocates or admits records.
func (p *Program) validateFrameSubset(ctx context.Context, validator xhandler.Validator, transactionStarted bool, selected []*Frame) error {
	collect := !transactionStarted && p.aggregateValidation()
	aggregate := &xhandler.Validation{}
	groups := map[*Record][]*Frame{}
	var order []*Record
	for _, frame := range selected {
		if frame == nil || frame.Record == nil || frame.Record.Auxiliary || frame.Action == xhandler.WriteDelete && !(transactionStarted && hasReconciliation(p.metadata.Root)) {
			continue
		}
		// An identity-only update writes nothing, so there is nothing for the
		// framework validator to check; entity Validate hooks still run for it.
		// Eligibility roots retain full validation before any exclusion decision.
		if !hasReconciliation(p.metadata.Root) && !frame.Record.writeEligibility && frame.Action == xhandler.WriteUpdate && !hasMutableFields(frame) {
			continue
		}
		if _, ok := groups[frame.Record]; !ok {
			order = append(order, frame.Record)
		}
		groups[frame.Record] = append(groups[frame.Record], frame)
	}
	for _, record := range order {
		frames := groups[record]
		values := reflect.MakeSlice(reflect.SliceOf(reflect.PointerTo(record.EntityType)), len(frames), len(frames))
		options := make([]xhandler.ValidationOptions, len(frames))
		for i, frame := range frames {
			values.Index(i).Set(frame.Entity)
			options[i] = p.validationOptions(frame, transactionStarted)
		}
		result, err := validator.Validate(ctx, values.Interface(), options)
		if err != nil {
			return fmt.Errorf("validate writer %s: %w", record.Path, err)
		}
		if err = result.Err(); err != nil {
			if !collect {
				return rhandler.ValidationPhaseFailure(fmt.Errorf("validate writer %s: %w", record.Path, err))
			}
			aggregate.Failed = true
			if aggregate.Code == 0 {
				aggregate.Code = result.Code
			}
			aggregate.Violations = append(aggregate.Violations, result.Violations...)
		}
	}
	if collect && aggregate.Err() != nil {
		return &initialSchemaViolations{validation: aggregate}
	}
	return aggregate.Err()
}

func (p *Program) callEntityHook(ctx context.Context, name string, frame *Frame) error {
	if name == "AfterQueue" && p.phaseSelectionAttempted {
		return p.callFiniteAfterQueueHook(ctx, frame)
	}
	return p.callEntityHookNative(ctx, name, frame)
}

func (p *Program) callEntityHookNative(ctx context.Context, name string, frame *Frame) error {
	if err := p.validateActionPolicyFacts(); err != nil {
		return err
	}
	if err := p.validateAuxiliaryFrameIdentity(frame); err != nil {
		return err
	}
	if invalid := p.skippedAuxiliaryAncestor(frame); invalid != nil {
		if name == "Init" {
			return nil
		}
		return fmt.Errorf("writer row at %s is nil", invalid.Location)
	}
	// Init can invalidate a later collection slot after frames were built.
	// Never pass that null to an entity hook or dereference its frame. Only the
	// approved root policy can retain it until initial structural validation.
	if p.metadata != nil && p.metadata.Root != nil && (p.metadata.Root.RootNullPolicy == "initial-validation" || (frame != nil && frame.Record != nil && frame.Record.NestedNullPolicy == "initial-validation")) && frame != nil && frame.Entity.IsValid() && frame.Entity.Kind() == reflect.Pointer && frame.Entity.IsNil() {
		if name == "Init" && (p.deferRootNullFrame(frame) || p.deferNestedNull(frame.Record, frame.Location, true)) {
			return nil
		}
		return fmt.Errorf("writer row at %s is nil", frame.Location)
	}
	if frame == nil || frame.SkippedDelete || !frame.Hook.IsValid() {
		return nil
	}
	method := frame.Hook.MethodByName(name)
	if !method.IsValid() {
		return nil
	}
	methodType := method.Type()
	if methodType.NumIn() != 3 {
		return fmt.Errorf("writer hook %s has %d arguments, want 3", name, methodType.NumIn())
	}
	state := p.entityHookState(methodType.In(2), frame)
	results := method.Call([]reflect.Value{reflect.ValueOf(ctx), frame.Entity, state})
	if err := methodError(name, results); err != nil {
		return err
	}
	if err := p.validateActionPolicyFacts(); err != nil {
		return err
	}
	if err := p.validateAuxiliaryFrameIdentity(frame); err != nil {
		return err
	}
	if name != "Init" {
		if invalid := p.skippedAuxiliaryAncestor(frame); invalid != nil {
			return fmt.Errorf("writer row at %s is nil", invalid.Location)
		}
	}
	return nil
}

func (p *Program) entityHookState(stateType reflect.Type, frame *Frame) reflect.Value {
	state := reflect.New(stateType).Elem()
	entityState := state.FieldByName("EntityState")
	if entityState.IsValid() {
		if location := entityState.FieldByName("Location"); location.IsValid() && location.CanSet() {
			location.SetString(frame.Location)
		}
		if previous := entityState.FieldByName("Previous"); previous.IsValid() && previous.CanSet() && frame.Previous.IsValid() && frame.Previous.Type().AssignableTo(previous.Type()) {
			previous.Set(frame.Previous)
		}
		if fields := entityState.FieldByName("PreviousFields"); fields.IsValid() && fields.CanSet() && frame.Previous.IsValid() {
			fields.Set(reflect.ValueOf(p.fieldsOf(frame.Previous.Elem().Type())))
		}
	}
	if output := state.FieldByName("Output"); output.IsValid() && output.CanSet() {
		value := reflect.ValueOf(p.output)
		if value.Type().AssignableTo(output.Type()) {
			output.Set(value)
		}
	}
	if parent := state.FieldByName("Parent"); parent.IsValid() && parent.CanSet() && frame.Parent != nil {
		value := frame.Parent.Entity
		if value.IsValid() && value.Type().AssignableTo(parent.Type()) {
			parent.Set(value)
		}
	}
	if original := state.FieldByName("Original"); original.IsValid() && original.CanSet() && frame.Original != nil {
		value := reflect.ValueOf(frame.Original)
		if value.Type().AssignableTo(original.Type()) || original.Type().Kind() == reflect.Interface && value.Type().Implements(original.Type()) {
			original.Set(value)
		}
	}
	return state
}

func methodError(name string, results []reflect.Value) error {
	if len(results) != 1 || !results[0].Type().Implements(reflect.TypeFor[error]()) {
		return fmt.Errorf("writer hook %s must return error", name)
	}
	if results[0].IsNil() {
		return nil
	}
	return results[0].Interface().(error)
}

func (p *Program) indexRecordCurrent(input reflect.Value, record *Record) error {
	if record.CurrentField >= 0 {
		if err := p.indexCurrent(record, input.Field(record.CurrentField)); err != nil {
			return err
		}
	}
	for _, relation := range record.Relations {
		if err := p.indexRecordCurrent(input, relation.Child); err != nil {
			return err
		}
	}
	return nil
}

func (p *Program) indexCurrent(record *Record, rows reflect.Value) error {
	if rows.Kind() == reflect.Pointer {
		if rows.IsNil() {
			return nil
		}
		wrapped := reflect.MakeSlice(reflect.SliceOf(rows.Type()), 1, 1)
		wrapped.Index(0).Set(rows)
		rows = wrapped
	}
	// Resolve the Current-to-entity field mapping once per collection instead
	// of two FieldByName lookups per field per row.
	type fieldCopy struct{ source, destination []int }
	copies := make([]fieldCopy, 0, len(record.Fields))
	var loaded fieldSet
	if p.queueObserver() != nil || (p.metadata != nil && p.metadata.Root != nil && (p.metadata.Root.writeEligibility || hasReconciliation(p.metadata.Root))) {
		loaded = fieldSet{}
		delete(p.previousFields, record)
	}
	if rows.Len() > 0 {
		sourceType := dereference(rows.Type().Elem())
		for _, field := range record.Fields {
			source, sourceOK := sourceType.FieldByName(field.Name)
			destination, destinationOK := record.EntityType.FieldByName(field.Name)
			if sourceOK && destinationOK && (source.Type.AssignableTo(destination.Type) ||
				destination.Type.Kind() == reflect.Pointer && source.Type.AssignableTo(destination.Type.Elem()) ||
				source.Type.Kind() == reflect.Pointer && source.Type.Elem().AssignableTo(destination.Type)) {
				copies = append(copies, fieldCopy{source: source.Index, destination: destination.Index})
				if loaded != nil {
					loaded[field.Name] = true
				}
			}
		}
	}
	for i := 0; i < rows.Len(); i++ {
		row := rows.Index(i)
		if row.IsNil() {
			continue
		}
		previous := reflect.New(record.EntityType)
		for _, copy := range copies {
			if err := assignLinkedValue(previous.Elem().FieldByIndex(copy.destination), row.Elem().FieldByIndex(copy.source)); err != nil {
				return fmt.Errorf("current writer row for %s: %w", record.Path, err)
			}
		}
		key, ok := record.loadedKey(previous.Elem())
		if !ok {
			return fmt.Errorf("current writer row for %s has incomplete identity", record.Path)
		}
		identity := rowIdentity{record: record, key: key}
		if _, exists := p.database.Rows[identity]; exists {
			return fmt.Errorf("current writer identity %q is duplicated", key.String())
		}
		p.database.Rows[identity] = previous
		p.database.ByRecord[record] = append(p.database.ByRecord[record], previous)
	}
	// Loaded evidence is published only after every selected conversion succeeds.
	if loaded != nil {
		if p.previousFields == nil {
			p.previousFields = map[*Record]fieldSet{}
		}
		p.previousFields[record] = loaded
	}
	return nil
}

func (p *Program) assemblePreviousRelations(record *Record) error {
	if record == nil {
		return nil
	}
	for _, relation := range record.Relations {
		if relation == nil || relation.Child == nil {
			continue
		}
		if err := p.assemblePreviousRelations(relation.Child); err != nil {
			return err
		}
		parents, children := p.previousRows(record), p.previousRows(relation.Child)
		// Index parents by their link values so assembly is linear in the
		// number of Current rows instead of parents×children reflect compares.
		parentsByLink := make(map[identityKey][]reflect.Value, len(parents))
		for _, parent := range parents {
			key := linkKey(parent.Elem(), relation.Links, true)
			parentsByLink[key] = append(parentsByLink[key], parent)
		}
		for _, child := range children {
			for _, parent := range parentsByLink[linkKey(child.Elem(), relation.Links, false)] {
				if !relationValuesEqual(parent.Elem(), child.Elem(), relation.Links) {
					continue
				}
				holder := parent.Elem().FieldByIndex(relation.Field)
				switch holder.Kind() {
				case reflect.Slice:
					holder.Set(reflect.Append(holder, child))
				case reflect.Pointer:
					if !holder.IsNil() && holder.Pointer() != child.Pointer() {
						return fmt.Errorf("current writer relation %s has multiple rows for a to-one holder", relation.Child.Path)
					}
					holder.Set(child)
				default:
					return fmt.Errorf("current writer relation %s holder is neither slice nor pointer", relation.Child.Path)
				}
				// Links may target non-unique parent columns; every matching parent
				// receives the child, so the scan deliberately continues.
			}
		}
	}
	return nil
}

func (p *Program) previousRows(record *Record) []reflect.Value {
	if rows, ok := p.database.ByRecord[record]; ok {
		return rows
	}
	// A snapshot assembled without per-record order (hand-built in tests)
	// falls back to a stable identity order.
	identities := make([]rowIdentity, 0, len(p.database.Rows))
	for identity := range p.database.Rows {
		if identity.record == record {
			identities = append(identities, identity)
		}
	}
	sort.Slice(identities, func(i, j int) bool { return identities[i].key.String() < identities[j].key.String() })
	result := make([]reflect.Value, 0, len(identities))
	for _, identity := range identities {
		result = append(result, p.database.Rows[identity])
	}
	return result
}

// linkKey renders one side of a relation's link values as a lookup key. Two
// values that linkedEqual considers equal always render the same key, so the
// key narrows candidates and relationValuesEqual confirms them.
func linkKey(entity reflect.Value, links []Link, parentSide bool) identityKey {
	var single [1]identityKey
	parts := single[:0]
	if len(links) > 1 {
		parts = make([]identityKey, 0, len(links))
	}
	for _, link := range links {
		field := link.Child
		if parentSide {
			field = link.Parent
		}
		parts = append(parts, scalarKey(indirect(entity.FieldByIndex(field.Index))))
	}
	return compositeKey(parts)
}

func relationValuesEqual(parent, child reflect.Value, links []Link) bool {
	if len(links) == 0 {
		return false
	}
	for _, link := range links {
		left, right := parent.FieldByIndex(link.Parent.Index), child.FieldByIndex(link.Child.Index)
		if !linkedEqual(left, right) {
			return false
		}
	}
	return true
}

func (p *Program) buildRecordFrames(ctx context.Context, binder xhandler.Binder, record *Record, rows reflect.Value, parent *Frame) error {
	if rows.Kind() == reflect.Pointer {
		if rows.IsNil() {
			return nil
		}
		return p.buildEntityFrame(ctx, binder, record, rows, parent, 0, false)
	}
	if rows.Kind() != reflect.Slice {
		return fmt.Errorf("writer role %s requires a record or collection, got %s", record.Path, rows.Type())
	}
	for i := 0; i < rows.Len(); i++ {
		if err := p.buildEntityFrame(ctx, binder, record, rows.Index(i), parent, i, true); err != nil {
			return err
		}
	}
	return nil
}

// rowLocation follows canonical Go holders, never transport aliases or tables.
func (p *Program) rowLocation(record *Record, parent *Frame, position int, indexed bool) string {
	location := record.Path
	if parent != nil {
		if relation := relationFor(parent.Record, record); relation != nil {
			location = parent.Location + "." + parent.Record.EntityType.FieldByIndex(relation.Field).Name
		}
	} else if input := reflect.ValueOf(p.input); input.IsValid() {
		typ := input.Type()
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if typ.Kind() == reflect.Struct && p.metadata.InputField >= 0 && p.metadata.InputField < typ.NumField() {
			location = typ.Field(p.metadata.InputField).Name
		}
	}
	if indexed {
		location += fmt.Sprintf("[%d]", position)
	}
	return location
}

// buildEntityFrame frames one row (a *T element or a to-one pointer holder)
// and recurses into its relations.
func (p *Program) buildEntityFrame(ctx context.Context, binder xhandler.Binder, record *Record, entity reflect.Value, parent *Frame, position int, indexed bool) error {
	{
		i := position
		if entity.IsNil() {
			if p.skipAuxiliaryNull(record, indexed) || (parent == nil && p.deferRootNull(record, i, indexed)) || p.deferNestedNull(record, p.rowLocation(record, parent, i, indexed), indexed) {
				return nil
			}
			return fmt.Errorf("writer row %d is nil", i)
		}
		p.captureActionPolicyFacts(record, entity)
		if err := p.validateActionPolicyFrameFacts(&Frame{Record: record, Entity: entity}); err != nil {
			return err
		}
		key, complete := record.key(entity.Elem())
		previous := p.database.Rows[rowIdentity{record: record, key: key}]
		if record.WriterIdentityPolicy == assignedUpdateIdentity && !requestedNonzeroIdentity(record, entity.Elem()) {
			previous = reflect.Value{}
		}
		if guard, ok := ctx.Value(scopedReplayKey{}).(map[rowIdentity]bool); ok && previous.IsValid() && guard[rowIdentity{record: record, key: key}] {
			return fmt.Errorf("scoped sequence replay cannot replace an existing insert identity")
		}
		if previous.IsValid() && parent != nil {
			relation := relationFor(parent.Record, record)
			if relation == nil {
				return fmt.Errorf("writer relation metadata for %s is unavailable", record.Path)
			}
			for _, link := range relation.Links {
				parentValue := parent.Entity.Elem().FieldByIndex(link.Parent.Index)
				previousValue := previous.Elem().FieldByIndex(link.Child.Index)
				if !linkedEqual(parentValue, previousValue) {
					return fmt.Errorf("writer relation %s matched Previous outside parent scope", record.Path)
				}
			}
		}
		deleteRequested := record.DeleteMarker != nil && supplied(entity.Elem(), *record.DeleteMarker) && boolValue(entity.Elem().FieldByIndex(record.DeleteMarker.Index))
		skipDelete := deleteRequested && complete && !previous.IsValid() && record.OnDeleteNotFound == "ignore" && record.ConcurrencyToken == nil
		if skipDelete && record.MutationPredicateGroup != nil {
			// Optional predicate declarations do not impose a guard when the
			// invocation supplies no active criteria. Active guards remain strict.
			predicateCtx, dialectErr := mutationPredicateContext(ctx, binder)
			if dialectErr != nil {
				return fmt.Errorf("mutation predicate %s dialect: %w", record.Path, dialectErr)
			}
			criteria, err := p.metadata.Predicates.Criteria(predicateCtx, binder, *record.MutationPredicateGroup)
			if err != nil {
				return fmt.Errorf("mutation predicate %s: %w", record.Path, err)
			}
			skipDelete = criteria == nil
		}
		action := xhandler.WriteInsert
		switch p.metadata.Operation {
		case "post":
			action = xhandler.WriteInsert
		case "put":
			if !previous.IsValid() && !skipDelete {
				return fmt.Errorf("put requires a matched complete identity")
			}
			action = xhandler.WriteUpdate
		case "patch":
			if previous.IsValid() && record.WriterActionPolicy != insertDeleteActionPolicy {
				action = xhandler.WriteUpdate
			}
		default:
			return fmt.Errorf("unsupported writer operation %q", p.metadata.Operation)
		}
		if parent == nil && record == p.metadata.Root && p.finiteRootDecision != nil {
			action = p.finiteRootDecision.action
			if action == xhandler.WriteUpdate && !previous.IsValid() {
				return fmt.Errorf("finite_reconciliation root UPDATE requires real Current at occurrence %d", position)
			}
		}
		noopMissing := p.metadata.Operation == "patch" && record.WriterIdentityPolicy == assignedUpdateIdentity && !previous.IsValid() && requestedNonzeroIdentity(record, entity.Elem()) && !deleteRequested
		if noopMissing {
			if err := p.checkMissingIdentityGuards(ctx, binder, record, entity.Elem()); err != nil {
				return err
			}
			action = xhandler.WriteUpdate
		}
		fields := livePresence(record, entity.Elem())
		if deleteRequested {
			if !complete || (!previous.IsValid() && !skipDelete) {
				return ErrDeleteNotFound
			}
			action = xhandler.WriteDelete
		}
		original := p.captureEntityOriginal(record, entity)
		frame := &Frame{framedEntity: reflect.ValueOf(entity.Interface()), holderPosition: position, holderIndexed: indexed, holderTracked: true, Entity: entity, Previous: previous, ExpectedToken: original.token, Fields: fields, SkippedDelete: skipDelete, NoopMissingIdentity: noopMissing, Action: action, Record: record, Location: p.rowLocation(record, parent, position, indexed), Parent: parent, Original: original, Hook: p.hooksByRecord[record]}
		p.captureActionPolicyOwner(frame)
		p.frames.Rows = append(p.frames.Rows, frame)
		for _, relation := range record.Relations {
			children := entity.Elem().FieldByIndex(relation.Field)
			// A generated cardinality-one relation is a pointer holder, which the
			// recursive builder frames directly without wrapping it in a slice.
			if children.Kind() != reflect.Slice && children.Kind() != reflect.Pointer {
				return fmt.Errorf("writer relation %s is not a collection", relation.Child.Path)
			}
			if err := p.buildRecordFrames(ctx, binder, relation.Child, children, frame); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateInsertIdentity(record *Record, entity reflect.Value, parent *Frame) error {
	for _, key := range record.Keys {
		value := entity.FieldByIndex(key.Index)
		resolved := !isNil(value) && (value.Kind() == reflect.Pointer || !value.IsZero() || supplied(entity, key))
		if resolved {
			continue
		}
		if record.Sequence != nil && record.Sequence.Name == key.Name {
			continue
		}
		produced := false
		if parent != nil {
			if relation := relationFor(parent.Record, record); relation != nil {
				for _, link := range relation.Links {
					produced = produced || link.Child.Name == key.Name
				}
			}
		}
		if !produced {
			return fmt.Errorf("writer insert %s identity field %s is unresolved and has no producer", record.Path, key.Name)
		}
	}
	return nil
}

func linkedEqual(left, right reflect.Value) bool {
	left, right = indirect(left), indirect(right)
	if !left.IsValid() || !right.IsValid() {
		return !left.IsValid() && !right.IsValid()
	}
	// Scalars compare without boxing; mixed widths of the same family compare
	// by value, which matches the previous textual fallback for numbers.
	switch leftKind, rightKind := scalarFamily(left.Kind()), scalarFamily(right.Kind()); {
	case leftKind == identityInt && rightKind == identityInt:
		return left.Int() == right.Int()
	case leftKind == identityUint && rightKind == identityUint:
		return left.Uint() == right.Uint()
	case leftKind == identityInt && rightKind == identityUint:
		return left.Int() >= 0 && uint64(left.Int()) == right.Uint()
	case leftKind == identityUint && rightKind == identityInt:
		return right.Int() >= 0 && uint64(right.Int()) == left.Uint()
	case leftKind == identityString && rightKind == identityString:
		return left.String() == right.String()
	case leftKind == identityBool && rightKind == identityBool:
		return left.Bool() == right.Bool()
	}
	if left.Type() == right.Type() {
		return reflect.DeepEqual(left.Interface(), right.Interface())
	}
	return fmt.Sprint(left.Interface()) == fmt.Sprint(right.Interface())
}

func scalarFamily(kind reflect.Kind) uint8 {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return identityInt
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return identityUint
	case reflect.String:
		return identityString
	case reflect.Bool:
		return identityBool
	}
	return 0
}

func (p *Program) applyInvariants(frame *Frame) error {
	if !frame.Previous.IsValid() || isPolicyInsert(frame) || p.isFiniteRootInsert(frame) {
		return nil
	}
	current, previous := frame.Entity.Elem(), frame.Previous.Elem()
	for name, fields := range frame.Record.Invariants {
		active := false
		for _, field := range fields {
			active = active || supplied(current, field)
		}
		if !active {
			continue
		}
		for _, field := range fields {
			if supplied(current, field) {
				continue
			}
			destination := current.FieldByIndex(field.Index)
			source := previousField(previous, frame.Record, &field)
			if !source.IsValid() || !destination.CanSet() || !source.Type().AssignableTo(destination.Type()) {
				return fmt.Errorf("invariant %s field %s cannot be backfilled", name, field.Name)
			}
			destination.Set(source)
			frame.Fields.force(field.Name)
		}
	}
	return nil
}

func (p *Program) checkConcurrency(frame *Frame) error {
	field := frame.Record.ConcurrencyToken
	if field == nil || (frame.Action != xhandler.WriteUpdate && frame.Action != xhandler.WriteDelete) || !frame.Previous.IsValid() {
		return nil
	}
	if frame.Original == nil || !frame.Original.Has(field.Name) || !frame.ExpectedToken.IsValid() {
		return &xhandler.Conflict{Entity: frame.Record.Path, Field: field.Name, Reason: "expected token is missing"}
	}
	expected := frame.ExpectedToken.Interface()
	actual := previousField(frame.Previous.Elem(), frame.Record, field)
	if !actual.IsValid() || !concurrencyTokenEqual(expected, actual.Interface()) {
		return &xhandler.Conflict{Entity: frame.Record.Path, Field: field.Name, Reason: "expected token differs from Previous"}
	}
	return nil
}

// previousField reuses the compiled entity index for canonical Previous rows
// produced by indexCurrent. Other layouts and manually assembled fields keep
// the name lookup; an entity index cannot be applied to a projection type.
func previousField(previous reflect.Value, record *Record, field *Field) reflect.Value {
	if previous.Type() == record.EntityType && len(field.Index) != 0 {
		return previous.FieldByIndex(field.Index)
	}
	return previous.FieldByName(field.Name)
}

func concurrencyTokenEqual(expected, actual any) bool {
	left, right := reflect.ValueOf(expected), reflect.ValueOf(actual)
	for left.IsValid() && left.Kind() == reflect.Pointer {
		if left.IsNil() {
			return !right.IsValid() || right.Kind() == reflect.Pointer && right.IsNil()
		}
		left = left.Elem()
	}
	for right.IsValid() && right.Kind() == reflect.Pointer {
		if right.IsNil() {
			return false
		}
		right = right.Elem()
	}
	if !left.IsValid() || !right.IsValid() || left.Type() != right.Type() {
		return false
	}
	if left.Type() == reflect.TypeOf(time.Time{}) {
		return left.Interface().(time.Time).Equal(right.Interface().(time.Time))
	}
	return reflect.DeepEqual(left.Interface(), right.Interface())
}

func Compile(component *spec.Component, inputType, outputType reflect.Type, operation string) (*Metadata, error) {
	if component == nil || component.RootView == nil || component.Settings == nil {
		return nil, fmt.Errorf("writer component metadata is incomplete")
	}
	operation = strings.ToLower(strings.TrimSpace(operation))
	if operation != "patch" && operation != "post" && operation != "put" {
		return nil, fmt.Errorf("generic writer requires patch, post, or put metadata, got %q", operation)
	}
	if err := component.Settings.ValidateProtectedFlushTables(); err != nil {
		return nil, err
	}
	if err := spec.ValidateInternalMutationRoot(component, operation); err != nil {
		return nil, err
	}
	metadata := &Metadata{Component: component.Clone(), Operation: operation, CurrentField: -1, OutputField: -1, Invariants: map[string][]Field{}}
	rootViewTag := ""
	for i := 0; i < inputType.NumField(); i++ {
		field := inputType.Field(i)
		parameter := field.Tag.Get("parameter")
		if (strings.Contains(parameter, "kind=body") || strings.Contains(parameter, "kind=internal")) && (field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Pointer) {
			metadata.InputField = i
			metadata.EntityType = dereference(field.Type)
			rootViewTag = field.Tag.Get("view")
		}
	}
	if metadata.EntityType == nil || metadata.EntityType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("writer input has no body entity collection")
	}
	metadata.Table = tagOption(rootViewTag, "table")
	auxiliaryRoot := component.RootView.Auxiliary || strings.EqualFold(tagOption(rootViewTag, "auxiliary"), "true")
	currentIndex, currentType, currentErr := currentInputField(inputType, metadata.Table, auxiliaryRoot, component.RootView.CanonicalName(), metadata.EntityType.Name())
	if currentErr != nil {
		return nil, currentErr
	}
	metadata.CurrentField, metadata.CurrentType = currentIndex, currentType
	if currentIndex >= 0 {
		metadata.Table = tagOption(inputType.Field(currentIndex).Tag.Get("view"), "table")
	}
	hookName := strings.TrimSpace(component.RootView.EntityHooks)
	if hookName == "" {
		hookName = tagOption(rootViewTag, "entityHooks")
	}
	if metadata.CurrentField < 0 && operation != "post" && !(auxiliaryRoot && len(component.RootView.Relations) == 0 && hookName != "") {
		return nil, fmt.Errorf("writer input has no current-state collection for %s in %s", metadata.EntityType, describeFields(inputType))
	}
	for i := 0; i < outputType.NumField(); i++ {
		field := outputType.Field(i)
		if (field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Pointer) && dereference(field.Type) == metadata.EntityType {
			metadata.OutputField = i
			break
		}
	}
	if metadata.OutputField < 0 {
		return nil, fmt.Errorf("writer output has no entity collection for %s", metadata.EntityType)
	}
	if component.RootView.Source != nil {
		if table := strings.TrimSpace(component.RootView.Source.Table); table != "" {
			metadata.Table = table
		}
	}
	if table := tagOption(rootViewTag, "table"); table != "" {
		metadata.Table = table
	}
	if metadata.Table == "" {
		return nil, fmt.Errorf("writer root table is required")
	}
	columns := map[string]*spec.Column{}
	for _, column := range component.RootView.Columns {
		if column != nil {
			columns[strings.ToLower(strings.TrimSpace(column.Name))] = column
			columns[strings.ToLower(strings.TrimSpace(column.Source))] = column
		}
	}
	has, _ := metadata.EntityType.FieldByName("Has")
	presence := entityMarker(metadata.EntityType)
	for i := 0; i < metadata.EntityType.NumField(); i++ {
		field := metadata.EntityType.Field(i)
		if field.Name == "Has" || field.PkgPath != "" {
			continue
		}
		sqlx := field.Tag.Get("sqlx")
		writerRole := strings.ToLower(strings.TrimSpace(field.Tag.Get("writer")))
		columnName := strings.TrimSpace(strings.Split(sqlx, ",")[0])
		column := columns[strings.ToLower(columnName)]
		if column == nil {
			column = columns[strings.ToLower(field.Name)]
		}
		// Transient scalar fields may be typed relation keys. Keep them in the
		// immutable record metadata so relation compilation and Current copying
		// reuse the generated projection without making them DML columns.
		if column == nil && columnName == "" && writerRole == "" {
			continue
		}
		compiled := Field{Name: field.Name, Column: columnName, Index: field.Index, RefDB: tagOption(sqlx, "refDb"), RefTable: tagOption(sqlx, "refTable"), RefColumn: tagOption(sqlx, "refColumn")}
		if has.Type != nil {
			markerType := dereference(has.Type)
			if marker, ok := markerType.FieldByName(field.Name); ok {
				compileMarker(&compiled, has, marker, presence)
			}
		}
		metadata.Fields = append(metadata.Fields, compiled)
		if typecatalog.SQLXPrimaryKey(field.Tag, column != nil && column.PrimaryKey) {
			metadata.Keys = append(metadata.Keys, compiled)
		}
		if !auxiliaryRoot && (column != nil && column.AutoIncrement || strings.Contains(strings.ToLower(sqlx), "autoincrement")) {
			compiled.AutoIncrement = true
			copy := compiled
			metadata.Sequence = &copy
		}
		if column != nil && column.DeleteMarker || writerRole == "delete" {
			copy := compiled
			metadata.DeleteMarker = &copy
		}
		if column != nil && column.ConcurrencyToken || writerRole == "concurrency" {
			copy := compiled
			metadata.ConcurrencyToken = &copy
		}
		if invariant := reflect.StructTag(field.Tag).Get("invariant"); invariant != "" {
			metadata.Invariants[invariant] = append(metadata.Invariants[invariant], compiled)
		}
	}
	if len(metadata.Keys) == 0 {
		return nil, fmt.Errorf("writer entity %s has no primary key", metadata.EntityType)
	}
	if !auxiliaryRoot && metadata.Sequence == nil && len(metadata.Keys) == 1 && numericField(metadata.EntityType, metadata.Keys[0]) {
		copy := metadata.Keys[0]
		metadata.Sequence = &copy
	}
	if hookName != "" {
		metadata.HookType = resolveHookType(component, hookName)
	}
	root := &Record{
		Name: component.RootView.CanonicalName(), Path: component.RootView.CanonicalName(), EntityType: metadata.EntityType,
		Auxiliary:    auxiliaryRoot,
		CurrentField: metadata.CurrentField, Table: metadata.Table, Keys: metadata.Keys, Fields: metadata.Fields,
		Sequence: metadata.Sequence, DeleteMarker: metadata.DeleteMarker, ConcurrencyToken: metadata.ConcurrencyToken,
		Invariants: metadata.Invariants, WriterActionPolicy: component.RootView.WriterActionPolicy, QueueContract: component.RootView.QueueContract, WriterIdentityPolicy: component.RootView.WriterIdentityPolicy, InsertValidationPresence: component.RootView.InsertValidationPresence, RootNullPolicy: component.RootView.RootNullPolicy, NestedNullPolicy: component.RootView.NestedNullPolicy, OnDeleteNotFound: component.RootView.OnDeleteNotFound, MutationPredicateGroup: component.RootView.MutationPredicateGroup, HookType: metadata.HookType,
	}
	if root.Name == "" {
		root.Name = metadata.EntityType.Name()
	}
	root.indexFields()
	if err := compileScopedSequences(root); err != nil {
		return nil, err
	}
	if metadata.Sequence != nil {
		root.Selector = metadata.Sequence.Name
	}
	if err := compileRelations(component, inputType, root, component.RootView); err != nil {
		return nil, err
	}
	if root.Auxiliary && len(root.Relations) == 0 && hookName != "" {
		if root.HookType == nil {
			return nil, fmt.Errorf("auxiliary lifecycle requires a linked leaf root hook")
		}
		hook := reflect.New(root.HookType)
		for _, name := range []string{"AfterSequence", "AfterQueue", "Recover", "RetryTransaction"} {
			if hook.MethodByName(name).IsValid() {
				return nil, fmt.Errorf("auxiliary component lifecycle does not support %s", name)
			}
		}
		if method := hook.MethodByName("Finalize"); method.IsValid() {
			typ := method.Type()
			if typ.NumIn() != 4 || typ.In(0) != reflect.TypeFor[context.Context]() || typ.In(1) != reflect.PointerTo(inputType) || typ.In(2) != reflect.PointerTo(outputType) || typ.In(3) != reflect.TypeFor[xhandler.Outcome]() || typ.NumOut() != 1 || typ.Out(0) != reflect.TypeFor[error]() || typ.IsVariadic() {
				return nil, fmt.Errorf("auxiliary lifecycle Finalize requires canonical Input, Output and Outcome")
			}
		}
		for _, name := range []string{"Init", "Validate"} {
			method := hook.MethodByName(name)
			if !method.IsValid() {
				return nil, fmt.Errorf("auxiliary component lifecycle requires %s", name)
			}
			typ := method.Type()
			if typ.NumIn() != 3 || typ.In(0) != reflect.TypeFor[context.Context]() || typ.In(1) != reflect.PointerTo(inputType) || typ.NumOut() != 1 || typ.Out(0) != reflect.TypeFor[error]() || typ.IsVariadic() || typ.In(2).Kind() != reflect.Struct {
				return nil, fmt.Errorf("auxiliary lifecycle %s requires canonical Input and LifecycleContext", name)
			}
			state := typ.In(2)
			previous, ok := state.FieldByName("Previous")
			if !ok || previous.Type != reflect.PointerTo(inputType) {
				return nil, fmt.Errorf("auxiliary lifecycle %s has invalid Previous type", name)
			}
			parent, ok := state.FieldByName("Parent")
			if !ok || parent.Type != reflect.TypeFor[*xhandler.NoParent]() {
				return nil, fmt.Errorf("auxiliary lifecycle %s has invalid Parent type", name)
			}
			output, ok := state.FieldByName("Output")
			if !ok || output.Type != reflect.PointerTo(outputType) {
				return nil, fmt.Errorf("auxiliary lifecycle %s has invalid Output type", name)
			}
		}
	}
	metadata.Root = root
	if err := validateQueueContracts(root, operation); err != nil {
		return nil, err
	}
	if err := validateWriterActionPolicy(root, operation); err != nil {
		return nil, err
	}
	if err := validateWriterIdentityPolicy(root, operation); err != nil {
		return nil, err
	}
	if err := validateMutationGroups(component, root); err != nil {
		return nil, err
	}
	if operation == "post" && hasOnDeleteNotFoundPolicy(root) {
		return nil, fmt.Errorf("onDeleteNotFound requires PATCH/PUT")
	}
	if hasMutationPredicate(root) {
		if operation == "post" {
			return nil, fmt.Errorf("mutation_predicate requires PATCH/PUT")
		}
		predicates, err := predicate.Compile(predicate.CompileInput{Component: component, InputType: inputType, Lookup: func(name string) (reflect.Type, error) {
			typ := resolveHookType(component, name)
			if typ == nil {
				return nil, fmt.Errorf("predicate type %s is not linked", name)
			}
			return typ, nil
		}})
		if err != nil {
			return nil, err
		}
		metadata.Predicates = predicates
	}
	if err := validateRootNullPolicy(root, inputType.Field(metadata.InputField).Type); err != nil {
		return nil, err
	}
	if err := validateAfterValidateInputHooks(root, inputType, outputType); err != nil {
		return nil, err
	}
	if err := validateAfterQueueInputHooks(root, inputType, outputType); err != nil {
		return nil, err
	}
	if err := validateAggregateHooks(root, inputType, outputType); err != nil {
		return nil, err
	}
	if err := validateWriteEligibilityHooks(metadata, outputType); err != nil {
		return nil, err
	}
	if err := validateReconciliation(metadata, inputType, outputType); err != nil {
		return nil, err
	}
	return metadata, nil
}

func describeFields(typeOf reflect.Type) string {
	parts := make([]string, 0, typeOf.NumField())
	for i := 0; i < typeOf.NumField(); i++ {
		field := typeOf.Field(i)
		parts = append(parts, field.Name+":"+field.Type.String()+":"+field.Tag.Get("parameter")+":"+field.Tag.Get("view"))
	}
	return strings.Join(parts, ";")
}

func compileRelations(component *spec.Component, inputType reflect.Type, parent *Record, view *spec.View) error {
	for i := 0; i < parent.EntityType.NumField(); i++ {
		structField := parent.EntityType.Field(i)
		viewTag := structField.Tag.Get("view")
		if viewTag == "" {
			continue
		}
		childType := dereference(structField.Type)
		if childType == nil || childType.Kind() != reflect.Struct {
			continue
		}
		name := strings.TrimSpace(strings.Split(viewTag, ",")[0])
		if name == "" {
			name = structField.Name
		}
		childView := relationView(view, structField.Name, name)
		child, err := compileRecord(component, inputType, name, parent.Path+"/"+structField.Name, childType, childView, viewTag)
		if err != nil {
			return err
		}
		relation := &Relation{Field: structField.Index, Child: child}
		for _, pair := range strings.Split(structField.Tag.Get("on"), ",") {
			left, right, ok := strings.Cut(strings.TrimSpace(pair), "=")
			if !ok {
				continue
			}
			left = strings.TrimSpace(strings.Split(left, ":")[0])
			right = strings.TrimSpace(strings.Split(right, ":")[0])
			parentField, parentOK := recordField(parent, left)
			childField, childOK := recordField(child, right)
			if !parentOK || !childOK {
				return fmt.Errorf("writer relation %s link %s=%s does not resolve", child.Path, left, right)
			}
			relation.Links = append(relation.Links, Link{Parent: parentField, Child: childField})
		}
		if len(relation.Links) == 0 {
			return fmt.Errorf("writer relation %s has no typed links", child.Path)
		}
		retainInheritedIdentity(parent, relation)
		parent.Relations = append(parent.Relations, relation)
	}
	return nil
}

// A shared primary key inherits its identity from the immediate parent. Keep
// the physical key and relation metadata, but do not allocate a second identity.
// Qualified and ambiguous ownership remains outside this classification.
func retainInheritedIdentity(parent *Record, relation *Relation) {
	child := relation.Child
	if parent.Auxiliary || child.Auxiliary || len(parent.Keys) != 1 || len(child.Keys) != 1 || child.Sequence == nil || child.Sequence.AutoIncrement {
		return
	}
	parentKey, childKey := parent.Keys[0], child.Keys[0]
	if !numericField(child.EntityType, childKey) || !sameIdentityField(childKey, *child.Sequence) || childKey.RefDB != "" ||
		!unqualifiedIdentity(parent.Table) || !unqualifiedIdentity(child.Table) || !unqualifiedIdentity(parentKey.Column) || !unqualifiedIdentity(childKey.Column) ||
		childKey.RefTable != parent.Table || childKey.RefColumn != parentKey.Column {
		return
	}
	field := child.EntityType.FieldByIndex(childKey.Index)
	if _, declared := field.Tag.Lookup("generator"); declared {
		return
	}
	for _, option := range strings.Split(field.Tag.Get("sqlx"), ",")[1:] {
		name, _, _ := strings.Cut(strings.TrimSpace(option), "=")
		if strings.EqualFold(name, "sequence") || strings.EqualFold(name, "generator") {
			return
		}
	}
	linked := 0
	for _, link := range relation.Links {
		if !sameIdentityField(link.Child, childKey) {
			continue
		}
		linked++
		if !sameIdentityField(link.Parent, parentKey) {
			return
		}
	}
	if linked == 1 {
		child.Sequence = nil
		child.Selector = ""
	}
}

func sameIdentityField(left, right Field) bool {
	return left.Name == right.Name && left.Column == right.Column && reflect.DeepEqual(left.Index, right.Index)
}

func unqualifiedIdentity(name string) bool {
	if name == "" {
		return false
	}
	for i, char := range name {
		if char == '_' || char == '$' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || i > 0 && char >= '0' && char <= '9' {
			continue
		}
		return false
	}
	return true
}

func compileRecord(component *spec.Component, inputType reflect.Type, name, path string, entityType reflect.Type, view *spec.View, viewTag string) (*Record, error) {
	record := &Record{Name: name, Path: path, EntityType: entityType, CurrentField: -1, Table: tagOption(viewTag, "table"), Auxiliary: strings.EqualFold(tagOption(viewTag, "auxiliary"), "true"), Invariants: map[string][]Field{}}
	record.RootNullPolicy = tagOption(viewTag, "rootNullPolicy")
	record.NestedNullPolicy = tagOption(viewTag, "nestedNullPolicy")
	if view != nil {
		record.Auxiliary = record.Auxiliary || view.Auxiliary
		record.WriterActionPolicy = view.WriterActionPolicy
		record.QueueContract = view.QueueContract
		record.WriterIdentityPolicy = view.WriterIdentityPolicy
		if view.RootNullPolicy != "" {
			record.RootNullPolicy = view.RootNullPolicy
		}
		if view.NestedNullPolicy != "" {
			record.NestedNullPolicy = view.NestedNullPolicy
		}
		record.InsertValidationPresence = view.InsertValidationPresence
		record.OnDeleteNotFound = view.OnDeleteNotFound
		record.MutationPredicateGroup = view.MutationPredicateGroup
		if view.Source != nil && strings.TrimSpace(view.Source.Table) != "" {
			record.Table = strings.TrimSpace(view.Source.Table)
		}
		record.HookType = resolveHookType(component, view.EntityHooks)
	}
	if record.HookType == nil {
		record.HookType = resolveHookType(component, tagOption(viewTag, "entityHooks"))
	}
	if record.Table == "" {
		return nil, fmt.Errorf("writer relation %s requires a table", path)
	}
	columns := map[string]*spec.Column{}
	if view != nil {
		for _, column := range view.Columns {
			if column != nil {
				columns[strings.ToLower(strings.TrimSpace(column.Name))] = column
				columns[strings.ToLower(strings.TrimSpace(column.Source))] = column
			}
		}
	}
	has, _ := entityType.FieldByName("Has")
	presence := entityMarker(entityType)
	for i := 0; i < entityType.NumField(); i++ {
		field := entityType.Field(i)
		if field.Name == "Has" || field.PkgPath != "" || field.Tag.Get("view") != "" {
			continue
		}
		sqlx := field.Tag.Get("sqlx")
		writerRole := strings.ToLower(strings.TrimSpace(field.Tag.Get("writer")))
		columnName := strings.TrimSpace(strings.Split(sqlx, ",")[0])
		if columnName == "" && writerRole == "" {
			continue
		}
		column := columns[strings.ToLower(columnName)]
		compiled := Field{Name: field.Name, Column: columnName, Index: field.Index, RefDB: tagOption(sqlx, "refDb"), RefTable: tagOption(sqlx, "refTable"), RefColumn: tagOption(sqlx, "refColumn")}
		if has.Type != nil {
			markerType := dereference(has.Type)
			if marker, ok := markerType.FieldByName(field.Name); ok {
				compileMarker(&compiled, has, marker, presence)
			}
		}
		record.Fields = append(record.Fields, compiled)
		if typecatalog.SQLXPrimaryKey(field.Tag, column != nil && column.PrimaryKey) {
			record.Keys = append(record.Keys, compiled)
		}
		if !record.Auxiliary && (column != nil && column.AutoIncrement || strings.Contains(strings.ToLower(sqlx), "autoincrement")) {
			compiled.AutoIncrement = true
			copy := compiled
			record.Sequence = &copy
		}
		if column != nil && column.DeleteMarker || writerRole == "delete" {
			copy := compiled
			record.DeleteMarker = &copy
		}
		if column != nil && column.ConcurrencyToken || writerRole == "concurrency" {
			copy := compiled
			record.ConcurrencyToken = &copy
		}
		if invariant := field.Tag.Get("invariant"); invariant != "" {
			record.Invariants[invariant] = append(record.Invariants[invariant], compiled)
		}
	}
	if len(record.Keys) == 0 {
		return nil, fmt.Errorf("writer entity %s has no primary key", entityType)
	}
	if !record.Auxiliary && record.Sequence == nil && len(record.Keys) == 1 && numericField(entityType, record.Keys[0]) {
		copy := record.Keys[0]
		record.Sequence = &copy
	}
	record.indexFields()
	if err := compileScopedSequences(record); err != nil {
		return nil, err
	}
	currentIndex, _, currentErr := currentInputField(inputType, record.Table, record.Auxiliary, name)
	if currentErr != nil {
		return nil, currentErr
	}
	record.CurrentField = currentIndex
	if record.Sequence != nil {
		record.Selector = strings.TrimPrefix(strings.TrimPrefix(path, component.RootView.CanonicalName()+"/"), "/")
		if record.Selector != "" {
			record.Selector += "/"
		}
		record.Selector += record.Sequence.Name
	}
	if err := compileRelations(component, inputType, record, view); err != nil {
		return nil, err
	}
	return record, nil
}

func relationView(parent *spec.View, holder, name string) *spec.View {
	if parent == nil {
		return nil
	}
	for _, relation := range parent.Relations {
		if relation != nil && relation.View != nil && (strings.EqualFold(relation.Holder, holder) || strings.EqualFold(relation.Name, name) || strings.EqualFold(relation.View.CanonicalName(), name)) {
			return relation.View
		}
	}
	return nil
}

func recordField(record *Record, name string) (Field, bool) {
	for _, field := range record.Fields {
		if strings.EqualFold(field.Name, name) || strings.EqualFold(field.Column, name) {
			return field, true
		}
	}
	return Field{}, false
}

func numericField(entityType reflect.Type, field Field) bool {
	typeOf := entityType.FieldByIndex(field.Index).Type
	for typeOf.Kind() == reflect.Pointer {
		typeOf = typeOf.Elem()
	}
	switch typeOf.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	}
	return false
}

func resolveHookType(component *spec.Component, expression string) reflect.Type {
	expression = strings.TrimSpace(strings.TrimPrefix(expression, "*"))
	if expression == "" {
		return nil
	}
	packagePath, typeName := component.Key.Scope, expression
	if index := strings.LastIndex(expression, "."); index >= 0 {
		alias := expression[:index]
		typeName = expression[index+1:]
		if strings.Contains(alias, "/") {
			packagePath = alias
		} else if component.TypeContext != nil {
			for _, item := range component.TypeContext.Imports {
				if item.Alias == alias {
					packagePath = item.Package
					break
				}
			}
		}
	}
	for _, candidate := range xunsafe.PackageTypes(packagePath) {
		candidate = dereference(candidate)
		if candidate != nil && candidate.Name() == typeName && candidate.PkgPath() == packagePath {
			return candidate
		}
	}
	return nil
}

func hasAction(actions []*Action, kind xhandler.WriteAction) bool {
	for _, action := range actions {
		if action != nil && action.Kind == kind {
			return true
		}
	}
	return false
}

func (m *Metadata) key(value reflect.Value) (identityKey, bool) {
	if m == nil || m.Root == nil {
		return identityKey{}, false
	}
	return m.Root.key(value)
}

func (r *Record) key(value reflect.Value) (identityKey, bool) {
	return r.keyValue(value, true)
}

func (r *Record) loadedKey(value reflect.Value) (identityKey, bool) {
	return r.keyValue(value, false)
}

func (r *Record) keyValue(value reflect.Value, requirePresence bool) (identityKey, bool) {
	var single [1]identityKey
	parts := single[:0]
	if len(r.Keys) > 1 {
		parts = make([]identityKey, 0, len(r.Keys))
	}
	for _, key := range r.Keys {
		field := value.FieldByIndex(key.Index)
		if isNil(field) {
			return identityKey{}, false
		}
		if requirePresence && field.Kind() != reflect.Pointer && field.IsZero() && !supplied(value, key) {
			return identityKey{}, false
		}
		parts = append(parts, scalarKey(indirect(field)))
	}
	return compositeKey(parts), true
}

type fieldSet map[string]bool

func (s fieldSet) Has(name string) bool { return s[name] }

type originalPresence struct {
	identityValues map[string]reflect.Value
	scopedNull     map[string]bool
	*presence
	available bool
	token     reflect.Value
}

func (p originalPresence) Available() bool { return p.available }

// A lifecycle may change a token through a setter (or mutate a supplied
// pointer in place). Keep the caller's original scalar value for both writer
// validation passes instead of rereading the working entity after Init.
func cloneTokenValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return value
		}
		copy := reflect.New(value.Type().Elem())
		copy.Elem().Set(value.Elem())
		return copy
	}
	copy := reflect.New(value.Type()).Elem()
	copy.Set(value)
	return copy
}

func presenceAvailable(entity reflect.Value) bool {
	marker := entity.FieldByName("Has")
	return marker.IsValid() && (!isNil(marker) || marker.Kind() != reflect.Pointer)
}

func supplied(entity reflect.Value, field Field) bool {
	if len(field.Has) == 0 {
		return true
	}
	if field.marker != nil && entity.CanAddr() {
		structPtr := unsafe.Pointer(entity.UnsafeAddr())
		// A missing holder means nothing was supplied; structology's IsSet would
		// assume the opposite, so guard explicitly.
		if !field.marker.CanUseHolder(structPtr) {
			return false
		}
		return field.marker.IsSet(structPtr, field.markerIndex)
	}
	marker := markerByIndex(entity, field.Has)
	return marker.IsValid() && marker.Kind() == reflect.Bool && marker.Bool()
}

// markerByIndex walks a compiled Has marker path, dereferencing pointers and
// returning an invalid value instead of panicking on a nil marker holder.
func markerByIndex(entity reflect.Value, index []int) reflect.Value {
	value := entity
	for _, i := range index {
		for value.Kind() == reflect.Pointer {
			if value.IsNil() {
				return reflect.Value{}
			}
			value = value.Elem()
		}
		if value.Kind() != reflect.Struct || i >= value.NumField() {
			return reflect.Value{}
		}
		value = value.Field(i)
	}
	return value
}

func allFields(typeOf reflect.Type) fieldSet {
	result := fieldSet{}
	for i := 0; i < typeOf.NumField(); i++ {
		if field := typeOf.Field(i); field.PkgPath == "" {
			result[field.Name] = true
		}
	}
	return result
}

func boolValue(value reflect.Value) bool {
	value = indirect(value)
	return value.IsValid() && value.Kind() == reflect.Bool && value.Bool()
}

func indirect(value reflect.Value) reflect.Value {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	return value
}

func dereference(typeOf reflect.Type) reflect.Type {
	for typeOf != nil && (typeOf.Kind() == reflect.Pointer || typeOf.Kind() == reflect.Slice) {
		typeOf = typeOf.Elem()
	}
	return typeOf
}

func tagOption(value, name string) string {
	for _, item := range strings.Split(value, ",") {
		key, actual, found := strings.Cut(strings.TrimSpace(item), "=")
		if found && strings.EqualFold(strings.TrimSpace(key), name) {
			return strings.TrimSpace(actual)
		}
	}
	return ""
}

func isNil(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func:
		return value.IsNil()
	}
	return false
}

func lookup[T any](ctx context.Context, binder xhandler.Binder, key xhandler.ValueKey) (T, error) {
	var zero T
	if binder == nil {
		return zero, fmt.Errorf("writer capability %s requires a binder", key)
	}
	value, found, err := binder.Lookup(ctx, key)
	if err != nil {
		return zero, err
	}
	result, ok := value.(T)
	if !found || !ok || isNil(reflect.ValueOf(result)) {
		return zero, fmt.Errorf("writer capability %s is unavailable", key)
	}
	return result, nil
}

func mutationPredicateContext(ctx context.Context, binder xhandler.Binder) (context.Context, error) {
	if dialectcontext.Dialect(ctx) != nil || binder == nil {
		return ctx, nil
	}
	value, found, err := binder.Lookup(ctx, xhandler.DMLKey)
	if err != nil {
		return nil, err
	}
	if !found {
		return ctx, nil
	}
	provider, ok := value.(rhandler.DialectProvider)
	if !ok {
		return ctx, nil
	}
	dialect, err := provider.Dialect(ctx)
	if err != nil {
		return nil, err
	}
	if dialect == nil {
		return ctx, nil
	}
	return dialectcontext.WithDialect(ctx, dialect), nil
}

func hasMutationPredicate(record *Record) bool {
	if record == nil {
		return false
	}
	if record.MutationPredicateGroup != nil {
		return true
	}
	for _, relation := range record.Relations {
		if hasMutationPredicate(relation.Child) {
			return true
		}
	}
	return false
}

func validateMutationGroups(component *spec.Component, record *Record) error {
	if record == nil {
		return nil
	}
	if record.OnDeleteNotFound != "" {
		if record.OnDeleteNotFound != "error" && record.OnDeleteNotFound != "ignore" {
			return fmt.Errorf("onDeleteNotFound must be error or ignore")
		}
		if record.Auxiliary || record.DeleteMarker == nil {
			return fmt.Errorf("onDeleteNotFound requires a writable delete marker")
		}
		if record.OnDeleteNotFound == "ignore" && len(record.Relations) > 0 {
			return fmt.Errorf("onDeleteNotFound ignore requires a leaf role")
		}
	}
	if record.MutationPredicateGroup != nil {
		if record.Auxiliary {
			return fmt.Errorf("mutation_predicate requires a writable role")
		}
		found := false
		for _, parameter := range component.Parameters {
			if parameter != nil {
				for _, definition := range parameter.Predicates {
					if definition != nil && definition.Group == *record.MutationPredicateGroup {
						found = true
					}
				}
			}
		}
		if !found {
			return fmt.Errorf("mutation_predicate group %d has no predicate inputs", *record.MutationPredicateGroup)
		}
	}
	for _, relation := range record.Relations {
		if err := validateMutationGroups(component, relation.Child); err != nil {
			return err
		}
	}
	return nil
}

func hasOnDeleteNotFoundPolicy(record *Record) bool {
	if record.OnDeleteNotFound != "" {
		return true
	}
	for _, relation := range record.Relations {
		if hasOnDeleteNotFoundPolicy(relation.Child) {
			return true
		}
	}
	return false
}
