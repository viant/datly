package collector

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/xunsafe"
)

// PartitionCopy creates an isolated collector for one partition. It retains
// relation identity for parent publication but never attaches concurrently;
// the canonical collector performs attachment during deterministic merge.
func (r *Collector) PartitionCopy() *Collector {
	dest := reflect.New(r.view.Schema.SliceType())
	dest.Elem().Set(reflect.MakeSlice(r.view.Schema.SliceType(), 0, 1))
	result := NewCollector(r.view, dest.Interface(), true)
	result.parent = r.parent
	result.relation = r.relation
	result.provenance = r.provenance
	return result
}

// AppendPartition merges one isolated partition into the canonical collector
// while preserving unmapped SQLx destinations used by relation keys.
func (r *Collector) AppendPartition(ctx context.Context, source *Collector) error {
	if r == nil || source == nil {
		return nil
	}
	visitor := r.Visitor(ctx)
	sourcePtr := xunsafe.AsPointer(source.DestPtr())
	for position := 0; position < source.slice.Len(sourcePtr); position++ {
		if r.provenance && source.provenance {
			if source.View() != r.View() {
				return fmt.Errorf("partition evidence belongs to a different prepared view")
			}
			r.evidence(r.indexCounter).fields = source.evidence(position).fields
			for holder, rows := range source.evidence(position).relations {
				r.evidence(r.indexCounter).relations[holder] = append([]*rowEvidence(nil), rows...)
			}
		}
		r.appendUnmapped(source, position)
		value := source.slice.ValuePointerAt(sourcePtr, position)
		r.appender.Append(value)
		if err := visitor(value); err != nil {
			return err
		}
	}
	return nil
}

func (r *Collector) appendUnmapped(source *Collector, position int) {
	for name, values := range source.values {
		if values == nil || position >= len(*values) {
			continue
		}
		target := r.values[name]
		if target == nil {
			buffer := make([]interface{}, 0)
			target = &buffer
			r.values[name] = target
		}
		*target = append(*target, (*values)[position])
		r.types[name] = source.types[name]
	}
}

// AppendSlice indexes a reducer-produced typed slice into this collector.
func (r *Collector) AppendSlice(ctx context.Context, rows any) error {
	value := reflect.ValueOf(rows)
	for value.IsValid() && (value.Kind() == reflect.Interface || value.Kind() == reflect.Ptr) {
		if value.IsNil() {
			return nil
		}
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Slice || value.Type() != r.view.Schema.SliceType() {
		return fmt.Errorf("partition reducer must return %s, got %T", r.view.Schema.SliceType(), rows)
	}
	visitor := r.Visitor(ctx)
	for i := 0; i < value.Len(); i++ {
		item := value.Index(i)
		r.appender.Append(item.Interface())
		if item.Kind() != reflect.Ptr && item.CanAddr() {
			item = item.Addr()
		}
		if err := visitor(item.Interface()); err != nil {
			return err
		}
	}
	return nil
}
