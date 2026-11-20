package writer

import (
	"context"
	"fmt"
	rhandler "github.com/viant/datly/runtime/handler"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/viant/datly/internal/associationtag"
	"github.com/viant/datly/spec"
	"github.com/viant/govalidator"
	sqlxio "github.com/viant/sqlx/io"
	xhandler "github.com/viant/xdatly/handler"
)

type associationCandidate struct {
	parent   uintptr
	member   identityKey
	field    Field
	ownerKey identityKey
	links    []Link
}

func compileAssociationIdentities(parent *Record, component *spec.Component) error {
	claimed := map[*Relation]bool{}
	for _, source := range parent.Relations {
		m := source.association
		if m == nil || m.insertIdentityRole == "" {
			continue
		}
		for _, target := range parent.Relations {
			if parent.EntityType.FieldByIndex(target.Field).Name != m.insertIdentityRole {
				continue
			}
			r := target.Child
			if !source.Child.Auxiliary || r.Auxiliary || source.Child.EntityType != r.EntityType || source.Child.Table != r.Table || len(r.Relations) != 0 || r.WriterActionPolicy != insertDeleteActionPolicy || r.QueueContract != "" || len(r.Keys) != 1 || r.Sequence == nil || !r.Sequence.AutoIncrement || r.Sequence.Name != r.Keys[0].Name || !slices.EqualFunc(source.Links, target.Links, func(a, b Link) bool {
				return sameIdentityField(a.Parent, b.Parent) && sameIdentityField(a.Child, b.Child)
			}) {
				return fmt.Errorf("association %s identity requires matching source and physical insert-delete leaf with one autoincrement key", source.Child.Path)
			}
			if !supportedAssociationKey(r.EntityType.FieldByIndex(r.Sequence.Index).Type) {
				return fmt.Errorf("association identity %s.%s must be a built-in signed integer", r.Path, r.Sequence.Name)
			}
			if claimed[target] || len(source.Links) != len(parent.Keys) || len(parent.Keys) == 0 {
				return fmt.Errorf("association identity requires an unambiguous owner and physical role")
			}
			for _, key := range parent.Keys {
				if !slices.ContainsFunc(source.Links, func(link Link) bool { return sameIdentityField(key, link.Parent) }) {
					return fmt.Errorf("association identity omits parent key %s", key.Name)
				}
			}
			claimed[target] = true
			if component.Settings == nil || !slices.Contains(component.Settings.ProtectedFlushTables, strings.ToLower(r.Table)) {
				return fmt.Errorf("association identity requires protected_flush_tables for %s", r.Table)
			}
			r.associationIdentityLeaf = true
			m.physicalIdentity = target
			break
		}
		if m.physicalIdentity == nil {
			return fmt.Errorf("association %s identity role %s is unresolved", source.Child.Path, m.insertIdentityRole)
		}
	}
	return nil
}

func associationPrefixTables(root *Record) map[string]bool {
	var tables map[string]bool
	var visit func(*Record)
	visit = func(r *Record) {
		for _, rel := range r.Relations {
			if m := rel.association; m != nil && m.physicalIdentity != nil {
				if tables == nil {
					tables = map[string]bool{}
				}
				tables[strings.ToLower(m.physicalIdentity.Child.Table)] = true
			}
			visit(rel.Child)
		}
	}
	visit(root)
	return tables
}

func (h *Handler) AssociationIdentityPrefix(_ associationtag.IdentityInsert, table string) bool {
	return h.metadata != nil && h.metadata.associationPrefixTables[table]
}

func (p *Program) captureAssociationIdentity(ctx context.Context, parent *Record, owner reflect.Value, rel *Relation, rows reflect.Value, path string) error {
	m := rel.association
	component := p.metadata.Component
	for _, route := range component.Routes {
		if route == nil {
			continue
		}
		target := xhandler.ComponentTarget{Scope: component.Key.Scope, Name: component.Key.Name, Method: route.Method, Path: route.Path}
		delta, ok := xhandler.LookupAssociationDelta(ctx, target, parent.EntityType, m.holder, path)
		if !ok {
			continue
		}
		for i := 0; i < rows.Len(); i++ {
			row := rows.Index(i)
			if row.IsNil() {
				return fmt.Errorf("null association %s", path)
			}
			field := indirect(row.Elem().FieldByIndex(m.member.Index))
			if !field.IsValid() || !delta.IsAdded(field.Interface()) {
				continue
			}
			if p.associationIdentityCandidates == nil {
				p.associationIdentityCandidates = map[frameIdentity]associationCandidate{}
			}
			k := frameIdentity{m.physicalIdentity.Child, row.Pointer()}
			if _, found := p.associationIdentityCandidates[k]; found {
				return fmt.Errorf("ambiguous association identity at %s", path)
			}
			p.associationIdentityCandidates[k] = associationCandidate{parent: owner.Addr().Pointer(), member: scalarKey(field), field: m.member, ownerKey: linkKey(owner, rel.Links, true), links: rel.Links}
		}
		break
	}
	return nil
}

func (p *Program) selectAssociationIdentity(ctx context.Context, binder xhandler.Binder, validator xhandler.Validator) error {
	if len(p.associationIdentityCandidates) == 0 {
		return nil
	}
	dml, err := lookup[xhandler.DML](ctx, binder, xhandler.DMLKey)
	if err != nil {
		return err
	}
	service, ok := dml.(associationtag.IdentityDML)
	if !ok {
		return nil
	}
	eligible, err := service.SupportsAssociationInsertIdentity(ctx)
	if err != nil || !eligible {
		return err
	}
	for _, frame := range p.frames.Rows {
		k := identityOfFrame(frame)
		c, found := p.associationIdentityCandidates[k]
		if !found || frame.Action != xhandler.WriteInsert {
			continue
		}
		if !c.matches(frame) || !frame.Entity.Elem().FieldByIndex(frame.Record.Sequence.Index).IsZero() {
			return fmt.Errorf("association insert lost its scoped owner, member or empty identity")
		}
		if !service.OwnsAssociationValidator(associationtag.NativeIdentity(), validator) {
			return fmt.Errorf("association insert identity requires its data owner’s native framework validator")
		}
		if rhandler.PhaseScopeFromContext(ctx).Observer() != nil {
			return fmt.Errorf("association insert identity cannot run with a phase observer")
		}
		if p.associationIdentityRows == nil {
			p.associationIdentityRows = map[frameIdentity]bool{}
		}
		p.associationIdentityRows[k] = true
	}
	return nil
}

func (c associationCandidate) matches(frame *Frame) bool {
	if frame.Parent == nil || frame.Parent.Entity.Pointer() != c.parent {
		return false
	}
	row := frame.Entity.Elem()
	member := indirect(row.FieldByIndex(c.field.Index))
	return member.IsValid() && scalarKey(member) == c.member && linkKey(frame.Parent.Entity.Elem(), c.links, true) == c.ownerKey && linkKey(row, c.links, false) == c.ownerKey
}

func (p *Program) hasAssociationIdentityRows(r *Record) bool {
	for k := range p.associationIdentityRows {
		if k.record == r {
			return true
		}
	}
	return false
}
func (p *Program) insertAssociationIdentity(ctx context.Context, dml xhandler.DML, frame *Frame) error {
	service, ok := dml.(associationtag.IdentityDML)
	if !ok {
		return fmt.Errorf("association insert capability unavailable")
	}
	key := frame.Record.Sequence
	v := frame.Entity.Elem().FieldByIndex(key.Index)
	if !v.IsZero() || !p.associationIdentityCandidates[identityOfFrame(frame)].matches(frame) {
		return fmt.Errorf("association identity, owner or member changed before insert")
	}
	if err := service.InsertAssociationIdentity(ctx, associationtag.NativeIdentity(), frame.Record.Table, frame.Entity.Interface(), v.Addr().Interface()); err != nil {
		return err
	}
	if err := p.advanceActionPolicyKey(frame, *key); err != nil {
		return err
	}
	return p.validateActionPolicyActions()
}

func supportedAssociationKey(t reflect.Type) bool {
	switch t {
	case reflect.TypeFor[int](), reflect.TypeFor[int8](), reflect.TypeFor[int16](), reflect.TypeFor[int32](), reflect.TypeFor[int64]():
		return true
	}
	return false
}

// A generated ID first exists when the row is inserted. Every public callback
// between allocation and the last such insert must be able to observe only its
// own physical row. Full validation of user input still runs before allocation.
func validateAssociationIdentityGraph(root *Record) error {
	if hasReconciliation(root) {
		return fmt.Errorf("association insert identity cannot combine with reconciliation")
	}
	var visit func(*Record) error
	visit = func(r *Record) error {
		if !r.Auxiliary && !r.associationIdentityLeaf {
			return fmt.Errorf("association insert identity cannot combine with unmapped physical role %s", r.Path)
		}
		if !r.Auxiliary {
			if err := validateAssociationLeaf(r); err != nil {
				return err
			}
			if _, ok := reflect.PointerTo(r.EntityType).MethodByName("OnInsert"); ok {
				return fmt.Errorf("association insert identity cannot combine with SQLX OnInsert on %s", r.Path)
			}
		}
		if r.HookType != nil {
			hook := reflect.PointerTo(r.HookType)
			for _, name := range []string{"AfterSequence", "AfterQueue", "WriteEligible", "ObservePhase", "ObserveQueueAttempt", "ReconcileInput"} {
				if _, ok := hook.MethodByName(name); ok {
					return fmt.Errorf("association insert identity cannot combine with %s on %s", name, r.Path)
				}
			}
			if method, ok := hook.MethodByName("AfterQueueRow"); ok {
				mt := method.Type
				if !r.associationIdentityLeaf || r.HookType.Kind() != reflect.Struct || r.HookType.NumField() != 0 || mt.IsVariadic() || mt.NumIn() != 3 || mt.In(1) != reflect.TypeFor[context.Context]() || mt.In(2) != reflect.PointerTo(r.EntityType) || mt.NumOut() != 1 || mt.Out(0) != reflect.TypeFor[error]() {
					return fmt.Errorf("AfterQueueRow requires a stateless physical identity leaf observer with (context.Context, *Row) error")
				}
				r.rowQueueObserver = true
			}
		}
		for _, rel := range r.Relations {
			if err := visit(rel.Child); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root)
}

func validateAssociationLeaf(record *Record) error {
	row := record.EntityType
	for i := 0; i < row.NumField(); i++ {
		f := row.Field(i)
		if len(govalidator.ParseTag(f.Tag.Get("validate")).Checks) > 0 {
			return fmt.Errorf("association identity cannot run a Go validation predicate on %s.%s before the insert", row, f.Name)
		}
		sql := sqlxio.ParseTag(f.Tag)
		if f.Tag.Get("refKey") != "" || sql.IsUnique || (record.Sequence != nil && f.Name == record.Sequence.Name && (sql.RefTable != "" || sql.NullifyEmpty || sql.Encoding != "")) {
			return fmt.Errorf("association identity cannot verify the SQL validation dependencies of %s.%s before the insert", row, f.Name)
		}
		typ := f.Type
		if typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		if f.Tag.Get("setMarker") == "true" && typ.Kind() == reflect.Struct {
			for j := 0; j < typ.NumField(); j++ {
				if typ.Field(j).Type != reflect.TypeFor[bool]() || len(govalidator.ParseTag(typ.Field(j).Tag.Get("validate")).Checks) > 0 {
					return fmt.Errorf("association row observer requires a scalar boolean presence marker")
				}
			}
			continue
		}
		if typ == reflect.TypeFor[time.Time]() {
			continue
		}
		if typ.PkgPath() != "" {
			return fmt.Errorf("association identity requires built-in scalar fields, unexpected %s.%s", row, f.Name)
		}
		switch typ.Kind() {
		case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		default:
			return fmt.Errorf("association row observer cannot expose nested field %s.%s", row, f.Name)
		}
	}
	return nil
}
