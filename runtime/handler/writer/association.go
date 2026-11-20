package writer

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/dialectcontext"
	"github.com/viant/sqlx"
	"reflect"
	"strings"

	"github.com/viant/datly/internal/associationtag"
	xhandler "github.com/viant/xdatly/handler"
)

// associationMapping belongs to one relation occurrence, even when several
// roles share its table. All Current field indexes are resolved at compilation.
type associationMapping struct {
	holder             string
	insertIdentityRole string
	physicalIdentity   *Relation
	member             Field
	presence           Field
	currentMember      []int
	currentLinks       []Link
	currentKeys        [][]int
}

func compileAssociation(holder reflect.StructField, input reflect.Type, parent *Record, rel *Relation, operation string) (*associationMapping, error) {
	desc, err := associationtag.Parse(holder)
	if err != nil || desc == nil {
		return nil, err
	}
	child := rel.Child
	if holder.Type.Elem().Kind() != reflect.Pointer {
		return nil, fmt.Errorf("association %s requires native writer pointer rows", child.Path)
	}
	member, ok := recordField(child, desc.Member.Name)
	if !ok || member.transient {
		return nil, fmt.Errorf("association %s member %s requires a mapped scalar", child.Path, desc.Member.Name)
	}
	mapping := &associationMapping{holder: holder.Name, member: member, insertIdentityRole: desc.InsertIdentityRole}
	if has, ok := parent.EntityType.FieldByName("Has"); ok {
		if marker, exists := dereference(has.Type).FieldByName(holder.Name); exists {
			mapping.presence.Name = holder.Name
			compileMarker(&mapping.presence, has, marker, entityMarker(parent.EntityType))
		}
	}
	// Auxiliary relations carry requests to their existing business lifecycle.
	if child.Auxiliary {
		return mapping, nil
	}
	if operation != "patch" || child.CurrentField < 0 || child.DeleteMarker == nil || len(child.Keys) == 0 || child.ConcurrencyToken != nil || len(child.Relations) != 0 {
		return nil, fmt.Errorf("association %s requires a PATCH leaf with Current, identity and an explicit delete marker; member-only inputs cannot supply concurrency tokens", child.Path)
	}
	if child.QueueContract != "" {
		return nil, fmt.Errorf("association %s requires conditional unlink; queue_contract %s does not support membership criteria", child.Path, child.QueueContract)
	}
	// Only an entire parent identity is sufficient to scope an unlink. A join
	// on a nonunique business column could cover links owned by other parents.
	if len(rel.Links) != len(parent.Keys) || len(parent.Keys) == 0 {
		return nil, fmt.Errorf("association %s must link every parent key", child.Path)
	}
	for _, key := range parent.Keys {
		matches := 0
		for _, link := range rel.Links {
			if sameIdentityField(key, link.Parent) {
				matches++
			}
		}
		if matches != 1 {
			return nil, fmt.Errorf("association %s must link parent key %s exactly once", child.Path, key.Name)
		}
	}
	currentRows := input.Field(child.CurrentField).Type
	if currentRows.Kind() != reflect.Slice {
		return nil, fmt.Errorf("association %s requires a collection Current", child.Path)
	}
	current := dereference(currentRows)
	resolve := func(f Field) ([]int, error) {
		source, ok := current.FieldByName(f.Name)
		if !ok || source.PkgPath != "" || len(source.Index) != 1 || source.Type.Kind() == reflect.Slice || dereference(source.Type) != dereference(child.EntityType.FieldByIndex(f.Index).Type) {
			return nil, fmt.Errorf("association %s Current must load %s with its declared type", child.Path, f.Name)
		}
		return source.Index, nil
	}
	if mapping.currentMember, err = resolve(member); err != nil {
		return nil, err
	}
	for _, link := range rel.Links {
		if link.Child.Name == member.Name {
			return nil, fmt.Errorf("association %s member is also a parent link", child.Path)
		}
		idx, err := resolve(link.Child)
		if err != nil {
			return nil, err
		}
		link.Child.Index = idx
		mapping.currentLinks = append(mapping.currentLinks, link)
	}
	for _, key := range child.Keys {
		idx, err := resolve(key)
		if err != nil {
			return nil, err
		}
		mapping.currentKeys = append(mapping.currentKeys, idx)
	}
	return mapping, nil
}

type associationKey struct{ scope, member identityKey }
type associationReads map[associationKey]reflect.Value

func (m *associationMapping) currentIndex(rows reflect.Value) (associationReads, error) {
	index := associationReads{}
	for i := 0; i < rows.Len(); i++ {
		row := indirect(rows.Index(i))
		if !row.IsValid() {
			continue
		}
		id := indirect(row.FieldByIndex(m.currentMember))
		if !id.IsValid() {
			continue // SQL NULL cannot match a primitive member.
		}
		key := associationKey{linkKey(row, m.currentLinks, false), scalarKey(id)}
		if _, exists := index[key]; exists {
			return nil, fmt.Errorf("association %s Current contains ambiguous scoped membership", m.holder)
		}
		index[key] = row
	}
	return index, nil
}

// The binder has resolved authorized Current and the input is still in its
// original order. Consume physical association intent before freezing the
// writer's ordinary identity and marker snapshots. All resulting rows use its
// standard validation, sequence allocation, lifecycle, and transaction.
func (p *Program) applyAssociationDeltas(ctx context.Context, input, rows reflect.Value) error {
	indexes := map[*Relation]associationReads{}
	type occurrence struct {
		relation *Relation
		owner    identityKey
	}
	// Only conflicting occurrences involving explicit deltas are disallowed.
	// Ordinary requests keep the writer's established duplicate policy.
	seen := map[occurrence]bool{}
	var visit func(*Record, reflect.Value, string) error
	visit = func(record *Record, rows reflect.Value, path string) error {
		if !record.hasAssociation {
			return nil
		}
		if rows.Kind() == reflect.Slice {
			for i := 0; i < rows.Len(); i++ {
				if err := visit(record, rows.Index(i), fmt.Sprintf("%s[%d]", path, i)); err != nil {
					return err
				}
			}
			return nil
		}
		row := indirect(rows)
		if !row.IsValid() {
			return nil
		}
		for _, rel := range record.Relations {
			children := row.FieldByIndex(rel.Field)
			holder := record.EntityType.FieldByIndex(rel.Field)
			childPath := path + "." + holder.Name
			if m := rel.association; m != nil && m.physicalIdentity != nil {
				if err := p.captureAssociationIdentity(ctx, record, row, rel, children, childPath); err != nil {
					return err
				}
			}
			if m := rel.association; m != nil {
				component := p.metadata.Component
				active := false
				for _, route := range component.Routes {
					if route == nil {
						continue
					}
					target := xhandler.ComponentTarget{Scope: component.Key.Scope, Name: component.Key.Name, Method: route.Method, Path: route.Path}
					delta, ok := xhandler.LookupAssociationDelta(ctx, target, record.EntityType, holder.Name, childPath)
					if !ok {
						continue
					}
					active = true
					if rel.Child.Auxiliary {
						// The admission hook prepares these rows. The owner occurrence
						// still participates in repeated-membership detection below.
						break
					}
					current := indexes[rel]
					if current == nil {
						var err error
						current, err = m.currentIndex(input.Field(rel.Child.CurrentField))
						if err != nil {
							return err
						}
						indexes[rel] = current
					}
					if err := p.translateAssociation(m, delta, current, rel, record, row, children); err != nil {
						return fmt.Errorf("%s: %w", childPath, err)
					}
					break
				}
				// A parallel entry for this parent may touch other fields. Only an
				// explicit second membership request can overlap the delta.
				ordinary := !children.IsNil() || len(m.presence.Has) > 0 && supplied(row, m.presence)
				if id, ok := record.key(row); (active || ordinary) && ok {
					key := occurrence{rel, id}
					if previous, repeated := seen[key]; repeated && (previous || active) {
						return fmt.Errorf("association %s has repeated parent identity %s", childPath, id.String())
					}
					seen[key] = active
				}
			}
			if err := visit(rel.Child, children, childPath); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(p.metadata.Root, rows, input.Type().Field(p.metadata.InputField).Name)
}

// PartitionAssociationRows lets a business admission hook consume the same
// normalized rows as the native writer. Each result aliases its input slice.
func PartitionAssociationRows[T any, K comparable](delta xhandler.AssociationDelta, rows []*T, member func(*T) K) (added, deleted []*T, err error) {
	if member == nil {
		return nil, nil, fmt.Errorf("missing association member accessor")
	}
	err = validateAssociationRows(delta, len(rows), func(i int) (any, bool) {
		if rows[i] == nil {
			return nil, false
		}
		return member(rows[i]), true
	})
	if err != nil {
		return nil, nil, err
	}
	n := delta.AddedCount()
	return rows[:n:n], rows[n:], nil
}

func validateAssociationRows(delta xhandler.AssociationDelta, count int, member func(int) (any, bool)) error {
	if delta == nil || delta.AddedCount() < 0 || delta.AddedCount() > count || delta.DeletedCount() != count-delta.AddedCount() {
		return fmt.Errorf("bound association rows disagree with declared intent")
	}
	seen := make(map[any]bool, count)
	for i := 0; i < count; i++ {
		id, present := member(i)
		if !present || id == nil || !reflect.ValueOf(id).Comparable() {
			return fmt.Errorf("missing association member")
		}
		add, remove := delta.IsAdded(id), delta.IsDeleted(id)
		if seen[id] || add == remove || add != (i < delta.AddedCount()) {
			return fmt.Errorf("association member disagrees with declared intent")
		}
		seen[id] = true
	}
	return nil
}

func (p *Program) translateAssociation(m *associationMapping, delta xhandler.AssociationDelta, current associationReads, rel *Relation, parent *Record, owner, rows reflect.Value) error {
	err := validateAssociationRows(delta, rows.Len(), func(i int) (any, bool) {
		row := indirect(rows.Index(i))
		if !row.IsValid() || !supplied(row, m.member) {
			return nil, false
		}
		value := indirect(row.FieldByIndex(m.member.Index))
		if !value.IsValid() {
			return nil, false
		}
		return value.Interface(), true
	})
	if err != nil {
		return err
	}
	selected := reflect.MakeSlice(rows.Type(), 0, rows.Len())
	scope := linkKey(owner, rel.Links, true)
	_, identified := parent.key(owner)
	for i := 0; i < rows.Len(); i++ {
		entry := rows.Index(i)
		row := indirect(entry)
		value := indirect(row.FieldByIndex(m.member.Index))
		id := scalarKey(value)
		add := i < delta.AddedCount()
		var stored reflect.Value
		if identified {
			stored = current[associationKey{scope, id}]
		}
		if stored.IsValid() && !relationValuesEqual(owner, stored, m.currentLinks) {
			return fmt.Errorf("ambiguous association parent scope")
		}
		if add && !stored.IsValid() {
			selected = reflect.Append(selected, entry)
		}
		if !add && stored.IsValid() {
			// Adopt only the physical identity; copying the stored record would
			// mark unrelated business fields as client-supplied.
			for j, key := range rel.Child.Keys {
				source := stored.FieldByIndex(m.currentKeys[j])
				if isNil(source) {
					return fmt.Errorf("stored association identity %s is null", key.Name)
				}
				if err := assignLinkedValue(row.FieldByIndex(key.Index), cloneTokenValue(source)); err != nil {
					return err
				}
				markSupplied(row, key)
			}
			marker := rel.Child.DeleteMarker
			if err := assignLinkedValue(row.FieldByIndex(marker.Index), reflect.ValueOf(true)); err != nil {
				return err
			}
			markSupplied(row, *marker)
			conditions := []associationCondition{{column: m.member.Column, expected: value.Interface()}}
			for j, link := range rel.Links {
				expected := indirect(stored.FieldByIndex(m.currentLinks[j].Child.Index))
				if !expected.IsValid() {
					return fmt.Errorf("stored association owner key %s is null", link.Child.Name)
				}
				conditions = append(conditions, associationCondition{column: link.Child.Column, expected: expected.Interface()})
			}
			if p.associationDeletes == nil {
				p.associationDeletes = map[frameIdentity][]associationCondition{}
			}
			p.associationDeletes[frameIdentity{rel.Child, entry.Pointer()}] = conditions
			selected = reflect.Append(selected, entry)
		}
	}
	rows.Set(selected)
	return nil
}

// Guard physical membership as well as the row PK. A surrogate identity can
// survive a concurrent reparent or member reassignment. Native CriteriaDML
// detects a zero match and rolls back the transaction, including prior inserts.
type associationCondition struct {
	column   string
	expected any
}

func associationDeleteCriteria(ctx context.Context, binder xhandler.Binder, conditions []associationCondition, existing *sqlx.Criteria) (*sqlx.Criteria, error) {
	predicateCtx, err := mutationPredicateContext(ctx, binder)
	if err != nil {
		return nil, err
	}
	dialect := dialectcontext.Dialect(predicateCtx)
	if dialect == nil {
		return nil, fmt.Errorf("association delete requires SQL dialect")
	}
	var expressions []string
	var args []any
	if existing != nil && existing.Expression != "" {
		expressions = append(expressions, "("+existing.Expression+")")
		args = append(args, existing.Placeholders...)
	}
	for _, c := range conditions {
		name, err := dialect.MappedColumnIdentifier(c.column)
		if err != nil {
			return nil, err
		}
		expressions = append(expressions, name+" = ?")
		args = append(args, c.expected)
	}
	return &sqlx.Criteria{Expression: strings.Join(expressions, " AND "), Placeholders: args}, nil
}
