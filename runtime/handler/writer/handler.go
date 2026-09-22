// Package writer provides the metadata-driven built-in mutation handler.
package writer

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
)

// Metadata is the immutable, component-specific plan interpreted by Handler.
// It contains no generated executable phase code.
type Metadata struct {
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
}

// Record is one writable role in a component graph.
type Record struct {
	Name             string
	Path             string
	Auxiliary        bool
	Selector         string
	EntityType       reflect.Type
	CurrentField     int
	Table            string
	Keys             []Field
	Fields           []Field
	Sequence         *Field
	DeleteMarker     *Field
	ConcurrencyToken *Field
	Invariants       map[string][]Field
	HookType         reflect.Type
	Relations        []*Relation
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
	inputType  reflect.Type
	outputType reflect.Type
	metadata   *Metadata
}

func New(component *spec.Component, inputType, outputType reflect.Type, operation string) (*Handler, error) {
	metadata, err := Compile(component, inputType, outputType, operation)
	if err != nil {
		return nil, err
	}
	return &Handler{inputType: inputType, outputType: outputType, metadata: metadata}, nil
}

func (h *Handler) InputType() reflect.Type  { return h.inputType }
func (h *Handler) OutputType() reflect.Type { return h.outputType }
func (*Handler) RequiresReadMetadata() bool { return true }

// Program is invocation-owned universal mutation state. The same type is used
// for every writer component; only Metadata and values differ.
type Program struct {
	metadata      *Metadata
	input         any
	output        any
	original      *OriginalInput
	database      *DatabaseSnapshot
	frames        *MutationFrames
	actions       *MutationActions
	validation    *FrameworkValidation
	hooks         *Hooks
	hook          reflect.Value
	hooksByRecord map[*Record]reflect.Value
	stage         Stage
	failed        bool
	finalized     bool
}

type OriginalInput struct{ Presence map[uintptr]originalPresence }
type DatabaseSnapshot struct{ Rows map[string]reflect.Value }
type MutationFrames struct{ Rows []*Frame }
type MutationActions struct{ Rows []*Action }
type FrameworkValidation struct{}
type Hooks struct{}
type Stage uint8

type Frame struct {
	Entity, Previous reflect.Value
	Fields           fieldSet
	Action           xhandler.WriteAction
	Record           *Record
	Parent           *Frame
	Original         xhandler.OriginalPresence
	Hook             reflect.Value
}

type Action struct {
	Kind   xhandler.WriteAction
	Entity reflect.Value
}

func (p *Program) allocate(ctx context.Context, sequencer xhandler.Sequencer, record *Record, roots reflect.Value) error {
	if record.Sequence != nil {
		if err := sequencer.Allocate(ctx, record.Table, roots.Interface(), record.Selector); err != nil {
			return fmt.Errorf("allocate %s: %w", record.Path, err)
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
	for _, frame := range p.frames.Rows {
		if frame == nil || frame.Parent == nil || frame.Record == nil {
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
			markSupplied(frame.Entity.Elem(), link.Child)
			frame.Fields[link.Child.Name] = true
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
	markerRoot := entity.FieldByName("Has")
	if !markerRoot.IsValid() || !markerRoot.CanSet() {
		return
	}
	if markerRoot.IsNil() {
		markerRoot.Set(reflect.New(markerRoot.Type().Elem()))
	}
	marker := markerRoot.Elem().FieldByName(field.Name)
	if marker.IsValid() && marker.CanSet() && marker.Kind() == reflect.Bool {
		marker.SetBool(true)
	}
}

func (h *Handler) Execute(ctx context.Context, invocation rhandler.Invocation) (any, error) {
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
	if err = program.prepare(ctx, invocation.Binder); err != nil {
		return program.output, err
	}
	return program.output, nil
}

func (h *Handler) CaptureInput(ctx context.Context, input any) (any, error) {
	program, err := h.program(input)
	if err != nil {
		return nil, err
	}
	if err = prepareReadIndexes(ctx, input); err != nil {
		return nil, err
	}
	return program, nil
}

func prepareReadIndexes(ctx context.Context, input any) error {
	method := reflect.ValueOf(input).MethodByName("PrepareReadIndexes")
	if !method.IsValid() {
		return nil
	}
	results := method.Call([]reflect.Value{reflect.ValueOf(ctx)})
	return methodError("PrepareReadIndexes", results)
}

func (h *Handler) FinalizeOutcome(ctx context.Context, invocation rhandler.Invocation, _ any, outcome xhandler.Outcome) error {
	program, _ := invocation.Snapshot.(*Program)
	if program == nil || !program.hook.IsValid() {
		return nil
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
		database: &DatabaseSnapshot{Rows: map[string]reflect.Value{}}, frames: &MutationFrames{},
		actions: &MutationActions{}, validation: &FrameworkValidation{}, hooks: &Hooks{}, hooksByRecord: map[*Record]reflect.Value{}, failed: true,
	}
	if h.metadata.Root != nil {
		result.prepareHooks(h.metadata.Root)
		result.hook = result.hooksByRecord[h.metadata.Root]
		entities := value.Elem().Field(h.metadata.InputField)
		if err := result.captureOriginal(h.metadata.Root, entities); err != nil {
			return nil, err
		}
	} else if h.metadata.HookType != nil {
		result.hook = reflect.New(h.metadata.HookType)
	}
	return result, nil
}

func (p *Program) captureOriginal(record *Record, rows reflect.Value) error {
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
			return fmt.Errorf("writer row %d is nil", i)
		}
		p.original.Presence[entity.Pointer()] = originalPresence{fieldSet: suppliedFields(entity.Elem(), record.Fields), available: presenceAvailable(entity.Elem())}
		for _, relation := range record.Relations {
			if err := p.captureOriginal(relation.Child, entity.Elem().FieldByIndex(relation.Field)); err != nil {
				return err
			}
		}
	}
	return nil
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
	if err := ctx.Err(); err != nil {
		return err
	}
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
	if err := p.buildRecordFrames(p.metadata.Root, entities, nil); err != nil {
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
	initialized := map[frameIdentity]bool{}
	for _, frame := range p.frames.Rows {
		if err = p.callEntityHook(ctx, "Init", frame); err != nil {
			return err
		}
		initialized[identityOfFrame(frame)] = true
		// Init is the supported phase for marker-aware business defaults and
		// sparse server-owned transitions. Merge newly set Has bits before
		// validation and action selection while retaining invariant evidence,
		// whose backfill deliberately does not mutate client presence markers.
		for name, present := range suppliedFields(frame.Entity.Elem(), frame.Record.Fields) {
			if present {
				frame.Fields[name] = true
			}
		}
	}
	// A lifecycle may implement atomic replacement by appending explicit
	// deletion rows derived from the assembled Previous graph. Rebuild frames
	// once after Init so those new rows participate in validation, ordering and
	// DML without invoking Init twice for the original topology.
	p.frames = &MutationFrames{}
	if err = p.buildRecordFrames(p.metadata.Root, entities, nil); err != nil {
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
		if !initialized[identityOfFrame(frame)] {
			if err = p.callEntityHook(ctx, "Init", frame); err != nil {
				return err
			}
		}
		for name, present := range suppliedFields(frame.Entity.Elem(), frame.Record.Fields) {
			if present {
				frame.Fields[name] = true
			}
		}
	}
	if err = p.validateFrames(ctx, validator, false); err != nil {
		return err
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
		if frame.Record.Auxiliary {
			continue
		}
		if frame.Action == xhandler.WriteUpdate && !hasMutableFields(frame) {
			continue
		}
		action := &Action{Kind: frame.Action, Entity: frame.Entity}
		if frame.Action == xhandler.WriteDelete {
			p.actions.Rows = append([]*Action{action}, p.actions.Rows...)
		} else {
			p.actions.Rows = append(p.actions.Rows, action)
		}
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
	if hasAction(p.actions.Rows, xhandler.WriteInsert) {
		sequencer, lookupErr := lookup[xhandler.Sequencer](ctx, binder, xhandler.SequencerKey)
		if lookupErr != nil {
			return lookupErr
		}
		if err = p.allocate(ctx, sequencer, p.metadata.Root, entities); err != nil {
			return err
		}
	}
	for _, frame := range p.frames.Rows {
		if err = p.callEntityHook(ctx, "AfterSequence", frame); err != nil {
			return err
		}
	}
	if err = p.reconcileLinks(true); err != nil {
		return err
	}
	if err = p.validateFrames(ctx, validator, true); err != nil {
		return err
	}
	dml, err := lookup[xhandler.DML](ctx, binder, xhandler.DMLKey)
	if err != nil {
		return err
	}
	for _, action := range p.actions.Rows {
		frame := p.frameFor(action.Entity)
		value := action.Entity.Interface()
		table := frame.Record.Table
		switch action.Kind {
		case xhandler.WriteInsert:
			err = dml.Insert(table, value)
		case xhandler.WriteUpdate:
			err = dml.Update(table, value)
		case xhandler.WriteDelete:
			err = dml.Delete(table, value)
		default:
			err = fmt.Errorf("unsupported writer action %q", action.Kind)
		}
		if err != nil {
			return fmt.Errorf("%s %s: %w", action.Kind, table, err)
		}
		if err = p.callEntityHook(ctx, "AfterQueue", frame); err != nil {
			return err
		}
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

func (p *Program) frameFor(entity reflect.Value) *Frame {
	for _, frame := range p.frames.Rows {
		if frame != nil && frame.Entity.IsValid() && frame.Entity.Pointer() == entity.Pointer() {
			return frame
		}
	}
	return nil
}

func hasMutableFields(frame *Frame) bool {
	if frame == nil || frame.Record == nil {
		return false
	}
	keys := map[string]bool{}
	for _, key := range frame.Record.Keys {
		keys[key.Name] = true
	}
	for name, present := range frame.Fields {
		if present && !keys[name] {
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
	options := xhandler.ValidationOptions{Action: frame.Action, Location: frame.Record.Path, Shallow: true}
	if frame.Previous.IsValid() {
		options.Previous = frame.Previous.Interface()
		options.PreviousFields = allFields(frame.Previous.Elem().Type())
		options.Fields = frame.Fields
	}
	graphReferences := p.satisfiedGraphReferences(frame)
	if frame.Action == xhandler.WriteUpdate && len(graphReferences) > 0 {
		// SQLX reference receipts are insert-only. For a sparse update whose new
		// FK value is proven to match an earlier insert in this ordered graph,
		// exclude only that reference field from the external database lookup.
		// The generated transaction and database FK still enforce the value.
		coverage := fieldSet{}
		for field, present := range frame.Fields {
			coverage[field] = present
		}
		for _, reference := range graphReferences {
			delete(coverage, reference.Field)
		}
		options.Fields = coverage
	}
	if frame.Action == xhandler.WriteInsert && frame.Parent != nil {
		if relation := relationFor(frame.Parent.Record, frame.Record); relation != nil {
			deferred := fieldSet{}
			for _, link := range relation.Links {
				if !transactionStarted && frame.Parent.Action == xhandler.WriteInsert || !linkValueResolved(frame.Parent.Entity.Elem().FieldByIndex(link.Parent.Index)) {
					deferred[link.Child.Name] = true
					continue
				}
				if transactionStarted && frame.Parent.Action == xhandler.WriteInsert && strings.EqualFold(link.Child.RefTable, frame.Parent.Record.Table) && strings.EqualFold(link.Child.RefColumn, link.Parent.Column) {
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
	current := frame.Entity.Elem()
	var result []xhandler.ValidationReference
	for _, field := range frame.Record.Fields {
		if field.RefTable == "" || field.RefColumn == "" {
			continue
		}
		value := current.FieldByIndex(field.Index)
		if !linkValueResolved(value) {
			continue
		}
		for _, candidate := range p.frames.Rows {
			if candidate == frame {
				break
			}
			if candidate == nil || candidate.Action != xhandler.WriteInsert || candidate.Record == nil || !strings.EqualFold(candidate.Record.Table, field.RefTable) || !candidate.Entity.IsValid() {
				continue
			}
			for _, parentField := range candidate.Record.Fields {
				if !strings.EqualFold(parentField.Column, field.RefColumn) {
					continue
				}
				parentValue := candidate.Entity.Elem().FieldByIndex(parentField.Index)
				if linkValueResolved(parentValue) && linkedEqual(value, parentValue) {
					result = append(result, xhandler.ValidationReference{Field: field.Name, Schema: field.RefDB, Table: field.RefTable, Column: field.RefColumn})
				}
				break
			}
		}
	}
	return result
}

func (p *Program) validateFrames(ctx context.Context, validator xhandler.Validator, transactionStarted bool) error {
	groups := map[*Record][]*Frame{}
	var order []*Record
	for _, frame := range p.frames.Rows {
		if frame == nil || frame.Record == nil || frame.Record.Auxiliary || frame.Action == xhandler.WriteDelete {
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
			return fmt.Errorf("validate writer %s: %w", record.Path, err)
		}
	}
	return nil
}

func (p *Program) callEntityHook(ctx context.Context, name string, frame *Frame) error {
	if frame == nil || !frame.Hook.IsValid() {
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
	state := reflect.New(methodType.In(2)).Elem()
	entityState := state.FieldByName("EntityState")
	if entityState.IsValid() {
		if previous := entityState.FieldByName("Previous"); previous.IsValid() && previous.CanSet() && frame.Previous.IsValid() && frame.Previous.Type().AssignableTo(previous.Type()) {
			previous.Set(frame.Previous)
		}
		if fields := entityState.FieldByName("PreviousFields"); fields.IsValid() && fields.CanSet() && frame.Previous.IsValid() {
			fields.Set(reflect.ValueOf(allFields(frame.Previous.Elem().Type())))
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
	results := method.Call([]reflect.Value{reflect.ValueOf(ctx), frame.Entity, state})
	return methodError(name, results)
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
	for i := 0; i < rows.Len(); i++ {
		row := rows.Index(i)
		if row.IsNil() {
			continue
		}
		previous := reflect.New(record.EntityType)
		for _, field := range record.Fields {
			source := row.Elem().FieldByName(field.Name)
			destination := previous.Elem().FieldByName(field.Name)
			if source.IsValid() && destination.IsValid() && destination.CanSet() && source.Type().AssignableTo(destination.Type()) {
				destination.Set(source)
			}
		}
		key, ok := record.loadedKey(previous.Elem())
		if !ok {
			return fmt.Errorf("current writer row for %s has incomplete identity", record.Path)
		}
		if _, exists := p.database.Rows[record.Path+"\x00"+key]; exists {
			return fmt.Errorf("current writer identity %q is duplicated", key)
		}
		p.database.Rows[record.Path+"\x00"+key] = previous
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
		for _, child := range children {
			for _, parent := range parents {
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
				break
			}
		}
	}
	return nil
}

func (p *Program) previousRows(record *Record) []reflect.Value {
	prefix := record.Path + "\x00"
	result := make([]reflect.Value, 0)
	for key, row := range p.database.Rows {
		if strings.HasPrefix(key, prefix) {
			result = append(result, row)
		}
	}
	return result
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

func (p *Program) buildRecordFrames(record *Record, rows reflect.Value, parent *Frame) error {
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
			return fmt.Errorf("writer row %d is nil", i)
		}
		key, complete := record.key(entity.Elem())
		previous := p.database.Rows[record.Path+"\x00"+key]
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
		action := xhandler.WriteInsert
		switch p.metadata.Operation {
		case "post":
			action = xhandler.WriteInsert
		case "put":
			if !previous.IsValid() {
				return fmt.Errorf("put requires a matched complete identity")
			}
			action = xhandler.WriteUpdate
		case "patch":
			if previous.IsValid() {
				action = xhandler.WriteUpdate
			}
		default:
			return fmt.Errorf("unsupported writer operation %q", p.metadata.Operation)
		}
		fields := suppliedFields(entity.Elem(), record.Fields)
		if record.DeleteMarker != nil && supplied(entity.Elem(), *record.DeleteMarker) && boolValue(entity.Elem().FieldByIndex(record.DeleteMarker.Index)) {
			if !previous.IsValid() || !complete {
				return fmt.Errorf("delete requires a matched complete identity")
			}
			action = xhandler.WriteDelete
		}
		original := p.original.Presence[entity.Pointer()]
		frame := &Frame{Entity: entity, Previous: previous, Fields: fields, Action: action, Record: record, Parent: parent, Original: original, Hook: p.hooksByRecord[record]}
		p.frames.Rows = append(p.frames.Rows, frame)
		for _, relation := range record.Relations {
			children := entity.Elem().FieldByIndex(relation.Field)
			// A generated cardinality-one relation is represented as a pointer;
			// normalize it for the universal recursive frame builder.
			if children.Kind() == reflect.Pointer {
				if children.IsNil() {
					continue
				}
				wrapped := reflect.MakeSlice(reflect.SliceOf(children.Type()), 1, 1)
				wrapped.Index(0).Set(children)
				children = wrapped
			}
			if children.Kind() != reflect.Slice {
				return fmt.Errorf("writer relation %s is not a collection", relation.Child.Path)
			}
			if err := p.buildRecordFrames(relation.Child, children, frame); err != nil {
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
	if left.Type() == right.Type() {
		return reflect.DeepEqual(left.Interface(), right.Interface())
	}
	return fmt.Sprint(left.Interface()) == fmt.Sprint(right.Interface())
}

func (p *Program) applyInvariants(frame *Frame) error {
	if !frame.Previous.IsValid() {
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
			source := previous.FieldByName(field.Name)
			if !source.IsValid() || !destination.CanSet() || !source.Type().AssignableTo(destination.Type()) {
				return fmt.Errorf("invariant %s field %s cannot be backfilled", name, field.Name)
			}
			destination.Set(source)
			frame.Fields[field.Name] = true
		}
	}
	return nil
}

func (p *Program) checkConcurrency(frame *Frame) error {
	field := frame.Record.ConcurrencyToken
	if field == nil || frame.Action != xhandler.WriteUpdate || !frame.Previous.IsValid() {
		return nil
	}
	current := frame.Entity.Elem()
	if !supplied(current, *field) {
		return &xhandler.Conflict{Entity: frame.Record.Path, Field: field.Name, Reason: "expected token is missing"}
	}
	expected := current.FieldByIndex(field.Index).Interface()
	actual := frame.Previous.Elem().FieldByName(field.Name)
	if !actual.IsValid() || !reflect.DeepEqual(expected, actual.Interface()) {
		return &xhandler.Conflict{Entity: frame.Record.Path, Field: field.Name, Reason: "expected token differs from Previous"}
	}
	return nil
}

func Compile(component *spec.Component, inputType, outputType reflect.Type, operation string) (*Metadata, error) {
	if component == nil || component.RootView == nil || component.Settings == nil {
		return nil, fmt.Errorf("writer component metadata is incomplete")
	}
	operation = strings.ToLower(strings.TrimSpace(operation))
	if operation != "patch" && operation != "post" && operation != "put" {
		return nil, fmt.Errorf("generic writer requires patch, post, or put metadata, got %q", operation)
	}
	metadata := &Metadata{Component: component.Clone(), Operation: operation, CurrentField: -1, OutputField: -1, Invariants: map[string][]Field{}}
	rootViewTag := ""
	for i := 0; i < inputType.NumField(); i++ {
		field := inputType.Field(i)
		parameter := field.Tag.Get("parameter")
		if strings.Contains(parameter, "kind=body") && (field.Type.Kind() == reflect.Slice || field.Type.Kind() == reflect.Pointer) {
			metadata.InputField = i
			metadata.EntityType = dereference(field.Type)
			rootViewTag = field.Tag.Get("view")
		}
	}
	if metadata.EntityType == nil || metadata.EntityType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("writer input has no body entity collection")
	}
	metadata.Table = tagOption(rootViewTag, "table")
	for i := 0; i < inputType.NumField(); i++ {
		field := inputType.Field(i)
		if field.Type.Kind() != reflect.Slice && field.Type.Kind() != reflect.Pointer || !strings.Contains(field.Tag.Get("parameter"), "kind=view") {
			continue
		}
		candidate := dereference(field.Type)
		sameRole := strings.EqualFold(strings.TrimSuffix(strings.TrimPrefix(field.Name, "Current"), "View"), strings.TrimSuffix(metadata.EntityType.Name(), "View"))
		sameTable := metadata.Table != "" && strings.EqualFold(tagOption(field.Tag.Get("view"), "table"), metadata.Table)
		if candidate != nil && candidate.Kind() == reflect.Struct && (sameRole || sameTable) {
			metadata.CurrentField, metadata.CurrentType = i, candidate
			metadata.Table = tagOption(field.Tag.Get("view"), "table")
			break
		}
	}
	if metadata.CurrentField < 0 && operation != "post" {
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
				compiled.Has = append(append([]int(nil), has.Index...), marker.Index...)
			}
		}
		metadata.Fields = append(metadata.Fields, compiled)
		if column != nil && column.PrimaryKey || strings.Contains(strings.ToLower(sqlx), "primarykey") {
			metadata.Keys = append(metadata.Keys, compiled)
		}
		if column != nil && column.AutoIncrement || strings.Contains(strings.ToLower(sqlx), "autoincrement") {
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
	if metadata.Sequence == nil && len(metadata.Keys) == 1 && numericField(metadata.EntityType, metadata.Keys[0]) {
		copy := metadata.Keys[0]
		metadata.Sequence = &copy
	}
	hookName := strings.TrimSpace(component.RootView.EntityHooks)
	if hookName == "" {
		hookName = tagOption(rootViewTag, "entityHooks")
	}
	if hookName != "" {
		metadata.HookType = resolveHookType(component, hookName)
	}
	root := &Record{
		Name: component.RootView.CanonicalName(), Path: component.RootView.CanonicalName(), EntityType: metadata.EntityType,
		Auxiliary:    component.RootView.Auxiliary || strings.EqualFold(tagOption(rootViewTag, "auxiliary"), "true"),
		CurrentField: metadata.CurrentField, Table: metadata.Table, Keys: metadata.Keys, Fields: metadata.Fields,
		Sequence: metadata.Sequence, DeleteMarker: metadata.DeleteMarker, ConcurrencyToken: metadata.ConcurrencyToken,
		Invariants: metadata.Invariants, HookType: metadata.HookType,
	}
	if root.Name == "" {
		root.Name = metadata.EntityType.Name()
	}
	if metadata.Sequence != nil {
		root.Selector = metadata.Sequence.Name
	}
	if err := compileRelations(component, inputType, root, component.RootView); err != nil {
		return nil, err
	}
	metadata.Root = root
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
		parent.Relations = append(parent.Relations, relation)
	}
	return nil
}

func compileRecord(component *spec.Component, inputType reflect.Type, name, path string, entityType reflect.Type, view *spec.View, viewTag string) (*Record, error) {
	record := &Record{Name: name, Path: path, EntityType: entityType, CurrentField: -1, Table: tagOption(viewTag, "table"), Auxiliary: strings.EqualFold(tagOption(viewTag, "auxiliary"), "true"), Invariants: map[string][]Field{}}
	if view != nil {
		record.Auxiliary = record.Auxiliary || view.Auxiliary
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
				compiled.Has = append(append([]int(nil), has.Index...), marker.Index...)
			}
		}
		record.Fields = append(record.Fields, compiled)
		if column != nil && column.PrimaryKey || strings.Contains(strings.ToLower(sqlx), "primarykey") {
			record.Keys = append(record.Keys, compiled)
		}
		if column != nil && column.AutoIncrement || strings.Contains(strings.ToLower(sqlx), "autoincrement") {
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
	if record.Sequence == nil && len(record.Keys) == 1 && numericField(entityType, record.Keys[0]) {
		copy := record.Keys[0]
		record.Sequence = &copy
	}
	for i := 0; i < inputType.NumField(); i++ {
		field := inputType.Field(i)
		if field.Type.Kind() != reflect.Slice && field.Type.Kind() != reflect.Pointer || !strings.Contains(field.Tag.Get("parameter"), "kind=view") {
			continue
		}
		candidate := dereference(field.Type)
		if candidate == nil || candidate.Kind() != reflect.Struct {
			continue
		}
		if strings.EqualFold(field.Name, "Current"+name) || strings.EqualFold(tagOption(field.Tag.Get("view"), "table"), record.Table) {
			record.CurrentField = i
			break
		}
	}
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

func (m *Metadata) key(value reflect.Value) (string, bool) {
	if m == nil || m.Root == nil {
		return "", false
	}
	return m.Root.key(value)
}

func (r *Record) key(value reflect.Value) (string, bool) {
	return r.keyValue(value, true)
}

func (r *Record) loadedKey(value reflect.Value) (string, bool) {
	return r.keyValue(value, false)
}

func (r *Record) keyValue(value reflect.Value, requirePresence bool) (string, bool) {
	parts := make([]string, len(r.Keys))
	for i, key := range r.Keys {
		field := value.FieldByIndex(key.Index)
		if isNil(field) {
			return "", false
		}
		if requirePresence && field.Kind() != reflect.Pointer && field.IsZero() && !supplied(value, key) {
			return "", false
		}
		parts[i] = fmt.Sprintf("%#v", indirect(field).Interface())
	}
	return strings.Join(parts, "\x00"), true
}

type fieldSet map[string]bool

func (s fieldSet) Has(name string) bool { return s[name] }

type originalPresence struct {
	fieldSet
	available bool
}

func (p originalPresence) Available() bool { return p.available }

func suppliedFields(entity reflect.Value, fields []Field) fieldSet {
	result := fieldSet{}
	for _, field := range fields {
		if supplied(entity, field) {
			result[field.Name] = true
		}
	}
	return result
}

func presenceAvailable(entity reflect.Value) bool {
	marker := entity.FieldByName("Has")
	return marker.IsValid() && (!isNil(marker) || marker.Kind() != reflect.Pointer)
}

func supplied(entity reflect.Value, field Field) bool {
	if len(field.Has) == 0 {
		return true
	}
	markerRoot := entity.FieldByName("Has")
	if !markerRoot.IsValid() || markerRoot.IsNil() {
		return false
	}
	marker := markerRoot.Elem().FieldByName(field.Name)
	return marker.IsValid() && marker.Kind() == reflect.Bool && marker.Bool()
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
