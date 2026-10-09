package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/spec"
	h "github.com/viant/xdatly/handler"
)

// OccurrenceRef is authority minted by one native invocation. A zero value or
// a reference retained from a different attempt cannot name an occurrence.
type OccurrenceRef struct{ ticket *reconciliationTicket }
type reconciliationTicket struct {
	owner    *reconciliationAttempt
	root     *Frame
	record   *Record
	frame    *Frame
	previous reflect.Value
	current  bool
	// Native bound-Current ordinal, retained independently of parent grouping.
	// -1 denotes a working occurrence or an older finite-mode ticket.
	currentOrdinal int
}

// ReconciliationAssignment changes only a declared scalar and optionally marks
// its source setter presence. Identity adoption uses AdoptCurrent instead.
type ReconciliationAssignment struct {
	Field       string
	Value       any
	MarkPresent bool
}
type ReconciliationSelection struct {
	Occurrence   OccurrenceRef
	AdoptCurrent OccurrenceRef
	Assignments  []ReconciliationAssignment
}
type ReconciliationRolePlan struct {
	Holder   string
	Selected []ReconciliationSelection
	Deletes  []OccurrenceRef
}
type ReconciliationRootPlan struct {
	Root        OccurrenceRef
	Assignments []ReconciliationAssignment
	Roles       []ReconciliationRolePlan
}
type ReconciliationPlan struct {
	Roots  []ReconciliationRootPlan
	Phases []ReconciliationPhasePlan
}

// Phase names resolve against compiled authoring authority. Hooks select
// occurrences and scalar values; they cannot supply actions, tables or ordering.
type ReconciliationPhasePlan struct {
	Phase string
	Roots []ReconciliationRootPhasePlan
}
type ReconciliationRootPhasePlan struct {
	Root     OccurrenceRef
	Selected []ReconciliationSelection
	Deletes  []OccurrenceRef
	Followup []ReconciliationAssignment
}

// ReconciliationObservation contains detached values for business selection.
// Row, Previous and allocation values convey evidence, never naming authority.
type ReconciliationObservation struct {
	Ref            OccurrenceRef
	Row            any
	Previous       any
	Preallocation  any
	Allocated      any
	ClientIdentity map[string]any
	Original       h.OriginalPresence
}
type ReconciliationRole struct {
	Holder           string
	Working, Current []ReconciliationObservation
}
type ReconciliationRoot struct {
	Ref                                     OccurrenceRef
	Row, Previous, Preallocation, Allocated any
	ClientIdentity                          map[string]any
	Original                                h.OriginalPresence
	Roles                                   []ReconciliationRole
}
type ReconciliationContext struct{ attempt *reconciliationAttempt }

// Roots preserves initialized root order, holder occurrence order and bound
// Current enumeration order. Every returned value is detached from writer state.
func (c ReconciliationContext) Roots() ([]ReconciliationRoot, error) {
	if c.attempt == nil || !c.attempt.active || c.attempt.observationsClosed {
		return nil, fmt.Errorf("reconciliation context is retired")
	}
	result := make([]ReconciliationRoot, 0, len(c.attempt.roots))
	for _, root := range c.attempt.roots {
		observation, e := c.attempt.observe(root.ref)
		if e != nil {
			return nil, e
		}
		entry := ReconciliationRoot{Ref: root.ref, Row: observation.Row, Previous: observation.Previous, Preallocation: observation.Preallocation, Allocated: observation.Allocated, ClientIdentity: observation.ClientIdentity, Original: observation.Original}
		for _, role := range root.roles {
			out := ReconciliationRole{Holder: role.role.declaration.Holder}
			for _, ref := range role.working {
				value, e := c.attempt.observe(ref)
				if e != nil {
					return nil, e
				}
				out.Working = append(out.Working, value)
			}
			for _, ref := range role.current {
				value, e := c.attempt.observe(ref)
				if e != nil {
					return nil, e
				}
				out.Current = append(out.Current, value)
			}
			entry.Roles = append(entry.Roles, out)
		}
		result = append(result, entry)
	}
	return result, nil
}

type reconciliationRoleMetadata struct {
	declaration spec.ReconciliationRole
	relation    *Relation
	fields      map[string]Field
}
type reconciliationMetadata struct {
	rootDecision *finiteRootDecisionMetadata
	fields       map[string]Field
	roles        []reconciliationRoleMetadata
}
type reconciliationAllocation struct {
	frame                    *Frame
	preallocation, allocated reflect.Value
	original                 h.OriginalPresence
}
type reconciliationRoleOccurrences struct {
	role             *reconciliationRoleMetadata
	working, current []OccurrenceRef
}
type reconciliationRootOccurrences struct {
	frame *Frame
	ref   OccurrenceRef
	roles []reconciliationRoleOccurrences
}
type reconciliationAttempt struct {
	observationsClosed bool
	selectionSealed    *finitePhasePlan
	selectionState     string
	selectionOutput    string
	rootPrepared       bool
	preparedState      string
	preparedOutput     string
	rootAllocated      bool
	allocatedRootKeys  []int64
	allocationState    string
	rootProjections    []finiteRootProjection
	projectionState    string
	active             bool
	roots              []reconciliationRootOccurrences
	tickets            map[*reconciliationTicket]bool
	allocations        map[*Frame]*reconciliationAllocation
}

func hasReconciliation(root *Record) bool { return root != nil && root.reconciliation != nil }

func validateReconciliation(metadata *Metadata, inputType, outputType reflect.Type) error {
	root := metadata.Root
	if err := rejectDescendantReconciliation(metadata.Component.RootView); err != nil {
		return err
	}
	if declaration := metadata.Component.RootView.Reconciliation; declaration != nil && declaration.SourcePhases != nil && declaration.Mode != "source-phases" {
		return fmt.Errorf("finite_reconciliation SourcePhases requires source-phases")
	}
	var visit func(*Record) error
	seen := map[*Record]bool{}
	visit = func(record *Record) error {
		if record == nil || seen[record] {
			return nil
		}
		seen[record] = true
		if record.HookType != nil {
			method := reflect.New(record.HookType).MethodByName("ReconcileInput")
			if method.IsValid() {
				if record != root || metadata.Component.RootView.Reconciliation == nil {
					return fmt.Errorf("ReconcileInput requires finite_reconciliation on the physical root")
				}
				typ := method.Type()
				if typ.IsVariadic() || typ.NumIn() != 4 || typ.NumOut() != 2 || typ.In(0) != reflect.TypeFor[context.Context]() || typ.In(1) != reflect.PointerTo(inputType) || typ.In(2) != reflect.PointerTo(outputType) || typ.In(3) != reflect.TypeFor[ReconciliationContext]() || typ.Out(0) != reflect.TypeFor[ReconciliationPlan]() || typ.Out(1) != reflect.TypeFor[error]() {
					return fmt.Errorf("ReconcileInput requires canonical Input/Output, ReconciliationContext and (ReconciliationPlan,error)")
				}
			}
		}
		for _, rel := range record.Relations {
			if e := visit(rel.Child); e != nil {
				return e
			}
		}
		return nil
	}
	if e := visit(root); e != nil {
		return e
	}
	declaration := metadata.Component.RootView.Reconciliation
	if declaration == nil {
		return nil
	}
	if metadata.Operation != "patch" || root.Auxiliary || root.Table == "" || root.DeleteMarker != nil || root.HookType == nil || !reflect.New(root.HookType).MethodByName("ReconcileInput").IsValid() {
		return fmt.Errorf("finite_reconciliation requires a physical non-delete PATCH root with ReconcileInput")
	}
	if len(root.ScopedSequences) != 0 {
		return fmt.Errorf("finite_reconciliation does not support scoped sequences")
	}
	for _, rel := range root.Relations {
		if len(rel.Child.ScopedSequences) != 0 || rel.Child.WriterIdentityPolicy != "" || rel.Child.WriterActionPolicy != "" || rel.Child.DeleteMarker != nil || rel.Child.ConcurrencyToken != nil || rel.Child.MutationPredicateGroup != nil || rel.Child.QueueContract != "" || rel.Child.OnDeleteNotFound != "" {
			return fmt.Errorf("finite_reconciliation unsupported child policy combination")
		}
	}
	if root.WriterIdentityPolicy != "" || root.WriterActionPolicy != "" || root.ConcurrencyToken != nil || root.MutationPredicateGroup != nil || root.QueueContract != "" || root.OnDeleteNotFound != "" {
		return fmt.Errorf("finite_reconciliation unsupported root policy combination")
	}
	if declaration.Mode == "source-phases" {
		return fmt.Errorf("finite_reconciliation source-phases is unavailable until phase/allocation/payload authority is complete")
	}
	if declaration.RootAction != "" {
		return fmt.Errorf("finite_reconciliation rootAction requires source-phases")
	}
	if declaration.Mode != "same-parent-root-first" {
		return fmt.Errorf("finite_reconciliation requires same-parent-root-first")
	}
	compiled := &reconciliationMetadata{}
	fields, e := reconciliationFields(root, declaration.RootFields, nil)
	if e != nil {
		return e
	}
	compiled.fields = fields
	if len(declaration.Roles) != len(root.Relations) {
		return fmt.Errorf("finite_reconciliation must declare every direct leaf holder in graph order")
	}
	adopts := 0
	for i, decl := range declaration.Roles {
		rel := root.Relations[i]
		holder := root.EntityType.FieldByIndex(rel.Field)
		if holder.Name != decl.Holder || holder.Type.Kind() != reflect.Slice || rel.Child.Auxiliary || len(rel.Child.Relations) != 0 || len(rel.Links) == 0 || len(rel.Child.Keys) == 0 || rel.Child.CurrentField < 0 {
			return fmt.Errorf("finite_reconciliation requires declared linked direct writable leaf collections with bound Current")
		}
		if decl.AdoptIdentity {
			adopts++
			if len(rel.Child.Keys) != 1 {
				return fmt.Errorf("finite_reconciliation adoption requires one complete identity")
			}
		}
		fields, e := reconciliationFields(rel.Child, decl.Fields, rel)
		if e != nil {
			return e
		}
		compiled.roles = append(compiled.roles, reconciliationRoleMetadata{decl, rel, fields})
	}
	if adopts > 1 {
		return fmt.Errorf("finite_reconciliation admits only one identity adoption role")
	}
	root.reconciliation = compiled
	return nil
}
func reconciliationFields(record *Record, names []string, rel *Relation) (map[string]Field, error) {
	result := map[string]Field{}
	for _, name := range names {
		field, ok := recordField(record, name)
		if !ok || result[name].Name != "" {
			return nil, fmt.Errorf("finite_reconciliation unknown or duplicate field %s.%s", record.Path, name)
		}
		for _, key := range record.Keys {
			if key.Name == name {
				return nil, fmt.Errorf("finite_reconciliation identity assignment is forbidden")
			}
		}
		typ := record.EntityType.FieldByIndex(field.Index).Type
		typ = dereference(typ)
		switch typ.Kind() {
		case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		default:
			return nil, fmt.Errorf("finite_reconciliation requires scalar fields")
		}
		if rel == nil && (field.RefTable != "" || field.RefDB != "") {
			return nil, fmt.Errorf("finite_reconciliation cannot change root reference fields")
		}
		result[name] = field
	}
	return result, nil
}

func (p *Program) captureReconciliationAllocation(ctx context.Context) error {
	if !hasReconciliation(p.metadata.Root) {
		return nil
	}
	if p.reconciliation != nil {
		return fmt.Errorf("finite_reconciliation requires a fresh capture")
	}
	if e := engine.VetoReconciliationReplay(ctx); e != nil {
		return e
	}
	owners := map[uintptr]*Frame{}
	for _, frame := range p.frames.Rows {
		if prior := owners[frame.Entity.Pointer()]; prior != nil {
			if prior.Record != frame.Record {
				return fmt.Errorf("finite_reconciliation unexplained cross-role pointer association")
			}
			if prior.Parent != frame.Parent && prior.Parent != nil && frame.Parent != nil {
				// Repeated source slots can name the same root storage before its
				// ID resolves. Exact storage equality is not reparenting.
				sameRoot := p.finiteRootDecision != nil && prior.Parent.Record == p.metadata.Root && frame.Parent.Record == p.metadata.Root && prior.Parent.Entity.IsValid() && frame.Parent.Entity.IsValid() && prior.Parent.Entity.Kind() == reflect.Pointer && frame.Parent.Entity.Kind() == reflect.Pointer && !prior.Parent.Entity.IsNil() && !frame.Parent.Entity.IsNil() && prior.Parent.Entity.Pointer() == frame.Parent.Entity.Pointer()
				if sameRoot {
					owners[frame.Entity.Pointer()] = frame
					continue
				}
				relation := relationFor(frame.Parent.Record, frame.Record)
				for _, link := range relation.Links {
					left := prior.Parent.Entity.Elem().FieldByIndex(link.Parent.Index)
					right := frame.Parent.Entity.Elem().FieldByIndex(link.Parent.Index)
					if !linkValueResolved(left) || !linkValueResolved(right) || !linkedEqual(left, right) {
						return fmt.Errorf("finite_reconciliation unexplained cross-parent pointer association")
					}
				}
			}
		}
		owners[frame.Entity.Pointer()] = frame
	}
	attempt := &reconciliationAttempt{active: true, tickets: map[*reconciliationTicket]bool{}, allocations: map[*Frame]*reconciliationAllocation{}}
	for _, frame := range p.frames.Rows {
		copy, e := detachedRow(frame.Entity)
		if e != nil {
			return e
		}
		attempt.allocations[frame] = &reconciliationAllocation{frame: frame, preallocation: reflect.ValueOf(copy), original: detachedOriginal{detachPresence(frame.Record, frame.Original), frame.Original.Available()}}
	}
	p.reconciliationFrames = append([]*Frame(nil), p.frames.Rows...)
	p.reconciliation = attempt
	return nil
}
func (a *reconciliationAttempt) mint(root *Frame, record *Record, frame *Frame, previous reflect.Value, current bool) OccurrenceRef {
	ticket := &reconciliationTicket{owner: a, root: root, record: record, frame: frame, previous: previous, current: current, currentOrdinal: -1}
	a.tickets[ticket] = true
	return OccurrenceRef{ticket}
}
func (a *reconciliationAttempt) observe(ref OccurrenceRef) (ReconciliationObservation, error) {
	ticket := ref.ticket
	var row reflect.Value
	out := ReconciliationObservation{Ref: ref}
	if ticket.current {
		row = ticket.previous
		out.Original = detachedOriginal{fieldSet{}, false}
	} else {
		row = ticket.frame.Entity
		allocation := a.allocations[ticket.frame]
		if allocation != nil {
			var e error
			out.Preallocation, e = detachedRow(allocation.preallocation)
			if e != nil {
				return out, e
			}
			out.Allocated, e = detachedRow(allocation.allocated)
			if e != nil {
				return out, e
			}
			out.Original = detachedOriginal{detachPresence(ticket.record, allocation.original), allocation.original.Available()}
			if original, ok := ticket.frame.Original.(originalPresence); ok {
				out.ClientIdentity = map[string]any{}
				for _, key := range ticket.record.Keys {
					value, e := detachedRow(original.identityValues[key.Name])
					if e != nil {
						return out, e
					}
					out.ClientIdentity[key.Name] = value
				}
			}
		}
	}
	var e error
	out.Row, e = detachedRow(row)
	if e != nil {
		return out, e
	}
	out.Previous, e = detachedRow(ticket.previous)
	return out, e
}
func (a *reconciliationAttempt) resolve(ref OccurrenceRef, root *Frame, record *Record, current *bool) (*reconciliationTicket, error) {
	ticket := ref.ticket
	if ticket == nil || ticket.owner != a || !a.active || !a.tickets[ticket] || ticket.root != root || ticket.record != record || current != nil && ticket.current != *current {
		return nil, fmt.Errorf("finite_reconciliation stale, forged, wrong-role or foreign-parent occurrence")
	}
	return ticket, nil
}

func (p *Program) reconcileInput(ctx context.Context) (err error) {
	if !hasReconciliation(p.metadata.Root) {
		return nil
	}
	if p.reconciliation == nil {
		if err = p.captureReconciliationAllocation(ctx); err != nil {
			return err
		}
	}
	attempt := p.reconciliation
	defer func() { attempt.active = false }()
	if err = ctx.Err(); err != nil {
		return err
	}
	// Distinct initialized root occurrences retain their own root association even
	// when the caller reused a pointer. No identity is recaptured or reallocated.
	seen := map[uintptr]bool{}
	inputRoots := reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField)
	for _, frame := range p.frames.Rows {
		if frame.Record != p.metadata.Root {
			continue
		}
		if seen[frame.Entity.Pointer()] {
			copy, e := detachedRow(frame.Entity)
			if e != nil {
				return e
			}
			frame.Entity = reflect.ValueOf(copy)
			frame.framedEntity = frame.Entity
			frame.Fields = livePresence(frame.Record, frame.Entity.Elem())
			inputRoots.Index(frame.holderPosition).Set(frame.Entity)
		}
		seen[frame.Entity.Pointer()] = true
	}
	for _, frame := range p.frames.Rows {
		allocation := attempt.allocations[frame]
		copy, e := detachedRow(frame.Entity)
		if e != nil {
			return e
		}
		allocation.allocated = reflect.ValueOf(copy)
	}
	for _, frame := range p.frames.Rows {
		if frame.Record != p.metadata.Root {
			continue
		}
		root := reconciliationRootOccurrences{frame: frame, ref: attempt.mint(frame, frame.Record, frame, frame.Previous, false)}
		for i := range p.metadata.Root.reconciliation.roles {
			role := &p.metadata.Root.reconciliation.roles[i]
			entries := reconciliationRoleOccurrences{role: role}
			for _, child := range p.frames.Rows {
				if child.Parent == frame && child.Record == role.relation.Child {
					entries.working = append(entries.working, attempt.mint(frame, child.Record, child, child.Previous, false))
				}
			}
			for _, previous := range p.database.ByRecord[role.relation.Child] {
				if err = p.requireReconciliationCurrent(role.relation, frame, previous, false); err != nil {
					return err
				}
				if relationValuesEqual(frame.Entity.Elem(), previous.Elem(), role.relation.Links) {
					entries.current = append(entries.current, attempt.mint(frame, role.relation.Child, nil, previous, true))
				}
			}
			root.roles = append(root.roles, entries)
		}
		attempt.roots = append(attempt.roots, root)
	}
	finish, e := engine.BeginReconciliation(ctx)
	if e != nil {
		return e
	}
	before, e := p.reconciliationState()
	if e != nil {
		return errors.Join(e, finish())
	}
	beforeOutput, e := immutableValues([]reflect.Value{reflect.ValueOf(p.output)})
	if e != nil {
		return errors.Join(e, finish())
	}
	var plan ReconciliationPlan
	func() {
		defer func() {
			err = errors.Join(err, finish())
			afterOutput, e := immutableValues([]reflect.Value{reflect.ValueOf(p.output)})
			err = errors.Join(err, e)
			if e == nil && beforeOutput != afterOutput {
				err = errors.Join(err, fmt.Errorf("ReconcileInput changed Output"))
			}
			after, e := p.reconciliationState()
			err = errors.Join(err, e)
			if e == nil && before != after {
				err = errors.Join(err, fmt.Errorf("ReconcileInput changed Input, Output, Current, Original, allocation or native associations"))
			}
		}()
		results := p.hook.MethodByName("ReconcileInput").Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(p.input), reflect.ValueOf(p.output), reflect.ValueOf(ReconciliationContext{attempt})})
		plan = results[0].Interface().(ReconciliationPlan)
		if !results[1].IsNil() {
			err = results[1].Interface().(error)
		}
	}()
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = p.applyReconciliationPlan(ctx, plan); err != nil {
		return err
	}
	return ctx.Err()
}
func (p *Program) requireReconciliationCurrent(rel *Relation, root *Frame, previous reflect.Value, scope bool) error {
	record := rel.Child
	for _, key := range record.Keys {
		if !p.previousFields[record].Has(key.Name) {
			return fmt.Errorf("finite_reconciliation requires loaded Current identity %s", key.Name)
		}
	}
	for _, link := range rel.Links {
		if !p.previousFields[record].Has(link.Child.Name) {
			return fmt.Errorf("finite_reconciliation requires loaded Current parent link %s", link.Child.Name)
		}
		if scope && !linkedEqual(root.Entity.Elem().FieldByIndex(link.Parent.Index), previous.Elem().FieldByIndex(link.Child.Index)) {
			return fmt.Errorf("finite_reconciliation Current is outside parent scope")
		}
	}
	if _, ok := record.loadedKey(previous.Elem()); !ok {
		return fmt.Errorf("finite_reconciliation Current identity is incomplete")
	}
	return nil
}
func (p *Program) reconciliationState() (string, error) {
	base, e := p.afterQueueInputState()
	if e != nil {
		return "", e
	}
	values := []reflect.Value{reflect.ValueOf(base), reflect.ValueOf(p.output).Elem().Field(p.metadata.OutputField)}
	// Bound request data and its presence remain observational. Runtime capability
	// injections have no data parameter tag and retain their own native guards.
	input := reflect.ValueOf(p.input).Elem()
	for i := 0; i < input.NumField(); i++ {
		field := input.Type().Field(i)
		parameter := field.Tag.Get("parameter")
		if field.PkgPath != "" {
			continue
		}
		if field.Tag.Get("setMarker") == "true" {
			values = append(values, input.Field(i))
			continue
		}
		for _, part := range strings.Split(parameter, ",") {
			switch part {
			case "kind=body", "kind=view", "kind=query", "kind=path", "kind=header", "kind=cookie", "kind=const", "kind=env":
				values = append(values, input.Field(i))
			}
		}
	}

	var collect func(*Record)
	collect = func(record *Record) {
		values = append(values, reflect.ValueOf(len(p.database.ByRecord[record])))
		for _, row := range p.database.ByRecord[record] {
			values = append(values, row)
		}
		for _, rel := range record.Relations {
			collect(rel.Child)
		}
	}
	collect(p.metadata.Root)
	for _, frame := range p.reconciliationFrames {
		allocation := p.reconciliation.allocations[frame]
		values = append(values, allocation.frame.Entity, allocation.frame.Previous, allocation.preallocation, allocation.allocated, reflect.ValueOf(frame.Original.Available()))
		for _, field := range frame.Record.Fields {
			values = append(values, reflect.ValueOf(frame.Original.Has(field.Name)))
			if original, ok := frame.Original.(originalPresence); ok {
				if value, exists := original.identityValues[field.Name]; exists {
					values = append(values, value)
				}
			}
		}
	}

	return immutableValues(values)
}

func (p *Program) applyReconciliationPlan(ctx context.Context, plan ReconciliationPlan) error {
	if plan.Phases != nil {
		return fmt.Errorf("finite_reconciliation phase selections require source-phases")
	}
	attempt := p.reconciliation
	if len(plan.Roots) != len(attempt.roots) {
		return fmt.Errorf("finite_reconciliation requires every root occurrence in unchanged order")
	}
	var effective []*Frame
	type holderChange struct{ destination, value reflect.Value }
	var changes []holderChange
	for i, rootPlan := range plan.Roots {
		if e := ctx.Err(); e != nil {
			return e
		}
		occurrence := &attempt.roots[i]
		root := occurrence.frame
		if rootPlan.Root != occurrence.ref {
			return fmt.Errorf("finite_reconciliation root order or authority changed")
		}
		row, e := detachedRow(root.Entity)
		if e != nil {
			return e
		}
		rootCopy := reflect.ValueOf(row)
		if e = p.applyReconciliationAssignments(rootCopy, root, p.metadata.Root.reconciliation.fields, rootPlan.Assignments, nil); e != nil {
			return e
		}
		replacement := *root
		replacement.Entity = root.Entity
		replacement.framedEntity = root.Entity
		replacement.Fields = livePresence(root.Record, root.Entity.Elem())
		newRoot := &replacement
		effective = append(effective, newRoot)
		if len(rootPlan.Roles) != len(occurrence.roles) {
			return fmt.Errorf("finite_reconciliation requires every declared holder")
		}
		for j, rolePlan := range rootPlan.Roles {
			role := occurrence.roles[j].role
			if rolePlan.Holder != role.declaration.Holder {
				return fmt.Errorf("finite_reconciliation role order changed")
			}
			holder := rootCopy.Elem().FieldByIndex(role.relation.Field)
			final := reflect.MakeSlice(holder.Type(), 0, len(rolePlan.Selected))
			selected := map[*reconciliationTicket]bool{}
			usedCurrent := map[*reconciliationTicket]bool{}
			var writes, deletes []*Frame
			for slot, selection := range rolePlan.Selected {
				ticket, e := attempt.resolve(selection.Occurrence, root, role.relation.Child, nil)
				if e != nil {
					return e
				}
				if selected[ticket] {
					return fmt.Errorf("finite_reconciliation occurrence selected twice")
				}
				selected[ticket] = true
				if ticket.current {
					usedCurrent[ticket] = true
				}
				frame, e := p.effectiveReconciliationFrame(ticket, newRoot, slot, false)
				if e != nil {
					return e
				}
				if selection.AdoptCurrent.ticket != nil {
					yes := true
					current, e := attempt.resolve(selection.AdoptCurrent, root, role.relation.Child, &yes)
					if e != nil {
						return e
					}
					if ticket.current || !role.declaration.AdoptIdentity {
						return fmt.Errorf("finite_reconciliation identity adoption is not declared")
					}
					if e = p.requireReconciliationCurrent(role.relation, newRoot, current.previous, true); e != nil {
						return e
					}
					usedCurrent[current] = true
					for _, key := range frame.Record.Keys {
						if e = assignLinkedValue(frame.Entity.Elem().FieldByIndex(key.Index), cloneTokenValue(current.previous.Elem().FieldByIndex(key.Index))); e != nil {
							return e
						}
						markSupplied(frame.Entity.Elem(), key)
					}
					frame.Previous = current.previous
					frame.Action = h.WriteUpdate
				}
				if e = p.applyReconciliationAssignments(frame.Entity, frame, role.fields, selection.Assignments, role.relation); e != nil {
					return e
				}
				if e = p.applyInvariants(frame); e != nil {
					return e
				}
				if frame.Previous.IsValid() {
					for _, currentRef := range occurrence.roles[j].current {
						if currentRef.ticket.previous.Pointer() == frame.Previous.Pointer() {
							usedCurrent[currentRef.ticket] = true
						}
					}
					if e = p.requireReconciliationCurrent(role.relation, newRoot, frame.Previous, true); e != nil {
						return e
					}
				}
				final = reflect.Append(final, frame.Entity)
				writes = append(writes, frame)
			}
			for slot, ref := range rolePlan.Deletes {
				yes := true
				ticket, e := attempt.resolve(ref, root, role.relation.Child, &yes)
				if e != nil {
					return e
				}
				if usedCurrent[ticket] || selected[ticket] {
					return fmt.Errorf("finite_reconciliation Current occurrence kept and deleted or deleted twice")
				}
				selected[ticket] = true
				frame, e := p.effectiveReconciliationFrame(ticket, newRoot, slot, true)
				if e != nil {
					return e
				}
				if e = p.requireReconciliationCurrent(role.relation, newRoot, frame.Previous, true); e != nil {
					return e
				}
				deletes = append(deletes, frame)
			}
			holder.Set(final)
			effective = append(effective, deletes...)
			effective = append(effective, writes...)
		}
		changes = append(changes, holderChange{root.Entity.Elem(), rootCopy.Elem()})
	}
	// Validate all data before publishing any holder. The allocation ledger and
	// Original are never rebuilt from the expanded business projection.
	for _, change := range changes {
		change.destination.Set(change.value)
	}
	p.frames = &MutationFrames{Rows: effective}
	p.graph = nil
	if e := p.reconcileLinks(true); e != nil {
		return e
	}
	if e := p.validateReconciliationOrder(); e != nil {
		return e
	}
	p.actions = &MutationActions{}
	p.queueItems = nil
	for _, frame := range effective {
		action := &Action{Kind: frame.Action, Entity: frame.Entity, frame: frame}
		if frame.Action != h.WriteUpdate || hasMutableFields(frame) {
			p.actions.Rows = append(p.actions.Rows, action)
		}
		if p.queueObserver() != nil {
			p.queueItems = append(p.queueItems, action)
		}
	}
	return nil
}
func (p *Program) effectiveReconciliationFrame(ticket *reconciliationTicket, root *Frame, slot int, deletion bool) (*Frame, error) {
	var value reflect.Value
	var original h.OriginalPresence
	action := h.WriteUpdate
	previous := ticket.previous
	if ticket.current {
		value = previous
		original = detachedOriginal{fieldSet{}, false}
	} else {
		value = ticket.frame.Entity
		original = ticket.frame.Original
		action = ticket.frame.Action
	}
	copy, e := detachedRow(value)
	if e != nil {
		return nil, e
	}
	entity := reflect.ValueOf(copy)
	frame := &Frame{Record: ticket.record, Entity: entity, framedEntity: entity, Previous: previous, Parent: root, Original: original, Action: action, Hook: p.hooksByRecord[ticket.record], Fields: livePresence(ticket.record, entity.Elem()), holderPosition: slot, holderIndexed: true, holderTracked: !deletion, Location: root.Location + "/" + ticket.record.Name + "[" + strconv.Itoa(slot) + "]"}
	if ticket.current {
		for _, field := range ticket.record.Fields {
			if p.previousFields[ticket.record].Has(field.Name) {
				frame.Fields.force(field.Name)
				markSupplied(entity.Elem(), field)
			}
		}
	}
	if deletion {
		frame.Action = h.WriteDelete
		frame.Location += "/internal-delete"
	}
	return frame, nil
}
func (p *Program) applyReconciliationAssignments(entity reflect.Value, frame *Frame, allowed map[string]Field, assignments []ReconciliationAssignment, rel *Relation) error {
	seen := map[string]bool{}
	for _, assignment := range assignments {
		field, ok := allowed[assignment.Field]
		if !ok || seen[assignment.Field] {
			return fmt.Errorf("finite_reconciliation undeclared or repeated field %s", assignment.Field)
		}
		seen[assignment.Field] = true
		destination := entity.Elem().FieldByIndex(field.Index)
		var value reflect.Value
		if assignment.Value == nil {
			if destination.Kind() != reflect.Pointer {
				return fmt.Errorf("finite_reconciliation nil scalar type mismatch")
			}
			value = reflect.Zero(destination.Type())
		} else {
			value = reflect.ValueOf(assignment.Value)
			if value.Type() != destination.Type() {
				return fmt.Errorf("finite_reconciliation scalar type mismatch for %s", field.Name)
			}
		}
		detached, e := detachedRow(value)
		if e != nil {
			return e
		}
		if detached == nil {
			destination.Set(reflect.Zero(destination.Type()))
		} else {
			destination.Set(reflect.ValueOf(detached))
		}
		if rel != nil {
			for _, link := range rel.Links {
				if field.Name == link.Child.Name && !linkedEqual(destination, frame.Parent.Entity.Elem().FieldByIndex(link.Parent.Index)) {
					return fmt.Errorf("finite_reconciliation cannot change parent scope")
				}
			}
		}
		if assignment.MarkPresent {
			markSupplied(entity.Elem(), field)
		}
	}
	return nil
}

// Fixed root-first traversal may not be silently reordered by reference sorting.
func (p *Program) validateReconciliationOrder() error {
	before := append([]*Frame(nil), p.frames.Rows...)
	if e := p.orderFramesByReferences(); e != nil {
		return e
	}
	for i, frame := range p.frames.Rows {
		if frame != before[i] {
			p.frames.Rows = before
			return fmt.Errorf("finite_reconciliation graph references cannot honor root-first declared-role order")
		}
	}
	return nil
}
func (p *Program) sealReconciliation() error {
	if !hasReconciliation(p.metadata.Root) {
		return nil
	}
	reflect.ValueOf(p.output).Elem().Field(p.metadata.OutputField).Set(reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField))
	snapshot, e := p.reconciliationState()
	if e != nil {
		return e
	}
	p.reconciliationSeal = snapshot
	return nil
}
func (p *Program) validateReconciliationSeal() error {
	if p.reconciliationSeal == "" {
		return nil
	}
	after, e := p.reconciliationState()
	if e != nil {
		return e
	}
	if after != p.reconciliationSeal {
		return fmt.Errorf("finite_reconciliation queued holders, native evidence or allocation ledger changed")
	}
	return nil
}

func rejectDescendantReconciliation(root *spec.View) error {
	seen := map[*spec.View]bool{}
	var visit func(*spec.View) error
	visit = func(view *spec.View) error {
		if view == nil || seen[view] {
			return nil
		}
		seen[view] = true
		if view != root && view.Reconciliation != nil {
			return fmt.Errorf("finite_reconciliation cannot be declared on a descendant")
		}
		for _, rel := range view.Relations {
			if rel != nil {
				if e := visit(rel.View); e != nil {
					return e
				}
			}
		}
		return nil
	}
	return visit(root)
}
