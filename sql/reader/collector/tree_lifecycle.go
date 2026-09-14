package collector

import (
	"fmt"
	"reflect"
	"unsafe"
)

// VisitRow visits actual materialized tree values child-first. Value-holder
// copies made while building the tree are the lifecycle targets, not old rows.
func (p *TreePlan) VisitRow(row any, visit func(any)) error {
	return p.visitRow(reflect.ValueOf(row), visit, map[unsafe.Pointer]bool{})
}

func (p *TreePlan) visitRow(row reflect.Value, visit func(any), active map[unsafe.Pointer]bool) error {
	for row.IsValid() && row.Kind() == reflect.Interface {
		if row.IsNil() {
			return nil
		}
		row = row.Elem()
	}
	if !row.IsValid() {
		return nil
	}
	if row.Kind() == reflect.Struct {
		if !row.CanAddr() {
			return fmt.Errorf("self-reference lifecycle row is not addressable")
		}
		row = row.Addr()
	}
	if row.Kind() != reflect.Ptr {
		return fmt.Errorf("self-reference lifecycle requires a struct pointer, got %s", row.Type())
	}
	if row.IsNil() {
		return nil
	}
	pointer := row.UnsafePointer()
	if active[pointer] {
		return fmt.Errorf("self-reference lifecycle contains a cycle")
	}
	active[pointer] = true
	defer delete(active, pointer)
	holder, err := p.holder.Get(row.Interface())
	if err != nil {
		return err
	}
	for i := 0; i < holder.Len(); i++ {
		if err = p.visitRow(holder.Index(i), visit, active); err != nil {
			return err
		}
	}
	visit(row.Interface())
	return nil
}
