package collector

import (
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/reader/readmeta"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
)

// hookTopology reconciles only object identity within existing attachment
// scopes. It never matches keys/values or interprets SQL/struct tags. Loaded
// fields are not immutable DB values: hooks may still change their values.
type hookTopology struct {
	called       atomic.Bool
	holders      map[*View]map[string]hookHolder
	roots        []*hookRow
	rootPointers map[unsafe.Pointer]*hookRow
	pointerRoots bool
	targets      map[*Collector][]any
}
type hookRow struct {
	source *data.View
	fields *readmeta.Fields
	edges  map[string]*hookEdge
}
type hookHolder struct {
	field *xunsafe.Field
	view  *View
	many  bool
}
type hookEdge struct {
	holder  hookHolder
	rows    map[unsafe.Pointer]*hookRow
	origins map[unsafe.Pointer]int
}
type hookIdentity struct {
	pointer unsafe.Pointer
	view    *View
}

func (r *Collector) hasRelationHooks() bool {
	model := (xshape.Runtime{}).Indirect(r.view.Schema.RowType())
	contract := reflect.TypeOf((*xhandler.OnRelationer)(nil)).Elem()
	if model != nil && model.Kind() == reflect.Struct && reflect.PointerTo(model).Implements(contract) {
		return true
	}
	for _, child := range r.relations {
		if child.hasRelationHooks() {
			return true
		}
	}
	return false
}

func (r *Collector) captureHookTopology() *hookTopology {
	result := &hookTopology{holders: map[*View]map[string]hookHolder{}, rootPointers: map[unsafe.Pointer]*hookRow{}, targets: map[*Collector][]any{}}
	result.captureTargets(r)
	values := r.hookRootRows()
	kind := r.destValue.Elem().Type().Elem().Kind()
	result.pointerRoots = kind == reflect.Pointer || kind == reflect.Interface
	counts := values.identities()
	ptr := xunsafe.AsPointer(r.DestPtr())
	for i := 0; i < r.Len(); i++ {
		row := r.slice.ValuePointerAt(ptr, i)
		captured := result.capture(r.view, row, r.evidence(i), map[hookIdentity]bool{})
		result.roots = append(result.roots, captured)
		_, pointer := values.at(i)
		if pointer != nil && counts[pointer] == 1 {
			result.rootPointers[pointer] = captured
		}
	}
	if !result.pointerRoots {
		result.excludeSharedValueRootChildren()
	}
	return result
}

// Freeze the complete collector batch before any child or parent hook runs.
// A child hook may hold an alias to its parent's root collection too.
func (s *hookTopology) captureTargets(collector *Collector) {
	ptr := xunsafe.AsPointer(collector.DestPtr())
	rows := make([]any, collector.Len())
	for i := range rows {
		rows[i] = collector.slice.ValuePointerAt(ptr, i)
	}
	s.targets[collector] = rows
	for _, child := range collector.relations {
		s.captureTargets(child)
	}
}

func (r *Collector) hookRootRows() hookRows {
	return hookRows{value: r.destValue.Elem(), many: true, model: (xshape.Runtime{}).Indirect(r.view.Schema.RowType())}
}

// Value-root slots are not identities. Even child pointers shared between such
// slots are ambiguous: a parent swap cannot establish which origin applies.
func (s *hookTopology) excludeSharedValueRootChildren() {
	type identity struct {
		holder  string
		view    *View
		pointer unsafe.Pointer
	}
	counts := map[identity]int{}
	for _, root := range s.roots {
		if root != nil {
			for name, edge := range root.edges {
				for pointer, count := range edge.origins {
					counts[identity{name, edge.holder.view, pointer}] += count
				}
			}
		}
	}
	for _, root := range s.roots {
		if root != nil {
			for name, edge := range root.edges {
				for pointer := range edge.rows {
					if counts[identity{name, edge.holder.view, pointer}] != 1 {
						delete(edge.rows, pointer)
					}
				}
			}
		}
	}
}

func (r *Collector) reconcileHookTopology(snapshot *hookTopology) {
	if snapshot == nil || !snapshot.called.Load() {
		return
	}
	ptr := xunsafe.AsPointer(r.DestPtr())
	values := r.hookRootRows()
	counts := values.identities()
	rows := make([]*rowEvidence, r.Len())
	for i := range rows {
		var prior *hookRow
		row := r.slice.ValuePointerAt(ptr, i)
		if snapshot.pointerRoots {
			_, pointer := values.at(i)
			if pointer != nil && counts[pointer] == 1 {
				prior = snapshot.rootPointers[pointer]
			}
		} else if i < len(snapshot.roots) && snapshot.roots[i] != nil {
			// Descendant pointer evidence may remain within the original holder
			// scope, but no scalar origin is asserted for an opaque value root.
			copy := *snapshot.roots[i]
			copy.source, copy.fields = nil, nil
			prior = &copy
		}
		rows[i] = snapshot.reconcile(prior, row, map[*hookRow]bool{})
	}
	r.rowEvidence = rows
}

func (s *hookTopology) viewHolders(view *View) map[string]hookHolder {
	if prior, ok := s.holders[view]; ok {
		return prior
	}
	result := map[string]hookHolder{}
	for _, relation := range view.Relations {
		if relation == nil || relation.IsOutput() || relation.Of == nil || relation.Of.View == nil || relation.HolderField == nil {
			continue
		}
		result[relation.Holder] = hookHolder{field: relation.HolderField, view: relation.Of.View, many: relation.Cardinality == spec.CardinalityMany}
	}
	if view.Tree != nil {
		result[view.Tree.holderField.Name] = hookHolder{field: view.Tree.holderField, view: view, many: true}
	}
	s.holders[view] = result
	return result
}

func (s *hookTopology) capture(view *View, row any, evidence *rowEvidence, active map[hookIdentity]bool) *hookRow {
	if evidence == nil || row == nil || reflect.ValueOf(row).Kind() == reflect.Pointer && reflect.ValueOf(row).IsNil() {
		return nil
	}
	key := hookIdentity{pointer: xunsafe.AsPointer(row), view: view}
	if active[key] {
		return nil
	}
	active[key] = true
	defer delete(active, key)
	result := &hookRow{source: evidence.source, fields: evidence.fields, edges: map[string]*hookEdge{}}
	for name, prior := range evidence.relations {
		holder, ok := s.viewHolders(view)[name]
		if !ok {
			continue
		}
		edge := &hookEdge{holder: holder, rows: map[unsafe.Pointer]*hookRow{}}
		result.edges[name] = edge
		values := holder.values(row)
		counts := values.identities()
		edge.origins = counts
		if values.len() != len(prior) {
			continue
		}
		for i := 0; i < values.len(); i++ {
			value, pointer := values.at(i)
			if pointer == nil || counts[pointer] != 1 {
				continue
			}
			edge.rows[pointer] = s.capture(holder.view, value, prior[i], active)
		}
	}
	return result
}

func (s *hookTopology) reconcile(snapshot *hookRow, row any, active map[*hookRow]bool) *rowEvidence {
	result := &rowEvidence{relations: map[string][]*rowEvidence{}}
	if snapshot == nil || active[snapshot] {
		return result
	}
	active[snapshot] = true
	defer delete(active, snapshot)
	result.source, result.fields = snapshot.source, snapshot.fields
	for name, edge := range snapshot.edges {
		values := edge.holder.values(row)
		counts := values.identities()
		children := make([]*rowEvidence, values.len())
		for i := range children {
			value, pointer := values.at(i)
			var prior *hookRow
			if pointer != nil && counts[pointer] == 1 {
				prior = edge.rows[pointer]
			}
			children[i] = s.reconcile(prior, value, active)
		}
		result.relations[name] = children
	}
	return result
}

// hookRows reads one already-compiled holder. Value elements intentionally have
// no identity: stable slot addresses cannot prove they were not replaced.
type hookRows struct {
	value reflect.Value
	many  bool
	model reflect.Type
}

func (h hookHolder) values(row any) hookRows {
	return hookRows{value: reflect.ValueOf(h.field.Value(xunsafe.AsPointer(row))), many: h.many, model: (xshape.Runtime{}).Indirect(h.view.Schema.RowType())}
}
func (r hookRows) len() int {
	if !r.value.IsValid() {
		return 0
	}
	if r.many {
		return r.value.Len()
	}
	if r.value.Kind() == reflect.Pointer && r.value.IsNil() {
		return 0
	}
	return 1
}
func (r hookRows) at(index int) (any, unsafe.Pointer) {
	value := r.value
	if r.many {
		value = value.Index(index)
	}
	for value.IsValid() && value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil, nil
		}
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || value.Type().Elem() != r.model {
		return nil, nil
	}
	return value.Interface(), value.UnsafePointer()
}
func (r hookRows) identities() map[unsafe.Pointer]int {
	result := map[unsafe.Pointer]int{}
	for i := 0; i < r.len(); i++ {
		_, pointer := r.at(i)
		if pointer != nil {
			result[pointer]++
		}
	}
	return result
}
