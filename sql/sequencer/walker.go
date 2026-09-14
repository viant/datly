package sequencer

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"unsafe"

	"github.com/viant/xunsafe"
)

type Walker struct {
	root *node
}

// cells retains writable leaf holders so the service can preflight a complete
// allocation before publishing any ID. Traversal remains owned by this Walker.
func (w *Walker) cells(ctx context.Context, n *node, value any) ([]*integerCell, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ptr := xunsafe.AsPointer(value)
	if ptr == nil {
		return nil, nil
	}
	switch n.kind {
	case nodeKindObject:
		return w.cells(ctx, n.children, n.xField.Interface(ptr))
	case nodeKindArray:
		var result []*integerCell
		for i := 0; i < n.xSlice.Len(ptr); i++ {
			cells, err := w.cells(ctx, n.children, n.xSlice.ValuePointerAt(ptr, i))
			if err != nil {
				return nil, err
			}
			result = append(result, cells...)
		}
		return result, nil
	case nodeKindLeaf:
		cell, err := integerCellOf(n.xField.Addr(ptr))
		if err != nil {
			return nil, err
		}
		return []*integerCell{cell}, nil
	}
	return nil, fmt.Errorf("unsupported sequence node")
}

func (w *Walker) CountEmpty(value interface{}) (int, error) {
	return w.countEmpty(w.root, value)
}

// Leaf returns a leaf value.
func (w *Walker) Leaf(value interface{}) (interface{}, error) {
	return w.leaf(w.root, value)
}

// EmptyLeaf returns the first record whose leaf selector currently has a zero value.
func (w *Walker) EmptyLeaf(value interface{}) (interface{}, error) {
	return w.emptyLeaf(w.root, value)
}

// Allocate allocate sequence.
func (w *Walker) Allocate(value interface{}, seq *Sequence) error {
	return w.allocate(w.root, value, seq)
}

func (w *Walker) allocate(aNode *node, value interface{}, seq *Sequence) error {
	ptr := xunsafe.AsPointer(value)
	var item interface{}
	switch aNode.kind {
	case nodeKindObject:
		item = aNode.xField.Interface(ptr)
		return w.allocate(aNode.children, item, seq)
	case nodeKindLeaf:
		item = aNode.xField.Addr(ptr)
	case nodeKindArray:
		sliceLen := aNode.xSlice.Len(ptr)
		for i := 0; i < sliceLen; i++ {
			item := aNode.xSlice.ValuePointerAt(ptr, i)
			if err := w.allocate(aNode.children, item, seq); err != nil {
				return err
			}
		}
		return nil
	}
	if item == nil {
		return fmt.Errorf("item was empty: %+v", aNode)
	}
	integer, err := integerCellOf(item)
	if err != nil {
		return err
	}
	empty, err := integer.isZero()
	if err != nil {
		return err
	}
	if empty {
		if err = integer.set(seq.Value); err != nil {
			return err
		}
		seq.Value += seq.IncrementBy
	}
	return nil
}

func (w *Walker) countEmpty(aNode *node, value interface{}) (int, error) {
	ptr := xunsafe.AsPointer(value)
	if (*unsafe.Pointer)(ptr) == nil {
		return 0, nil
	}
	var result int
	var item interface{}
	switch aNode.kind {
	case nodeKindObject:
		item = aNode.xField.Interface(ptr)
		return w.countEmpty(aNode.children, item)
	case nodeKindLeaf:
		item = aNode.xField.Addr(ptr)
	case nodeKindArray:
		sliceLen := aNode.xSlice.Len(ptr)
		for i := 0; i < sliceLen; i++ {
			item := aNode.xSlice.ValuePointerAt(ptr, i)
			count, err := w.countEmpty(aNode.children, item)
			if err != nil {
				return 0, err
			}
			result += count
		}
		return result, nil
	}
	if item == nil {
		return 0, fmt.Errorf("item was empty: %+v", aNode)
	}
	integer, err := integerCellOf(item)
	if err != nil {
		return 0, err
	}
	empty, err := integer.isZero()
	if err != nil {
		return 0, err
	}
	if empty {
		return 1, nil
	}
	return 0, nil
}

func (w *Walker) leaf(aNode *node, value interface{}) (interface{}, error) {
	ptr := xunsafe.AsPointer(value)
	if ptr == nil {
		return nil, nil
	}
	var item interface{}
	switch aNode.kind {
	case nodeKindObject:
		item = aNode.xField.Interface(ptr)
		return w.leaf(aNode.children, item)
	case nodeKindLeaf:
		return value, nil
	case nodeKindArray:
		sliceLen := aNode.xSlice.Len(ptr)
		for i := 0; i < sliceLen; i++ {
			item := aNode.xSlice.ValuePointerAt(ptr, i)
			first, err := w.leaf(aNode.children, item)
			if err != nil {
				return nil, err
			}
			if first != nil {
				return first, nil
			}
		}
		return nil, nil
	}
	return item, nil
}

func (w *Walker) emptyLeaf(aNode *node, value interface{}) (interface{}, error) {
	ptr := xunsafe.AsPointer(value)
	if ptr == nil {
		return nil, nil
	}
	var item interface{}
	switch aNode.kind {
	case nodeKindObject:
		item = aNode.xField.Interface(ptr)
		return w.emptyLeaf(aNode.children, item)
	case nodeKindLeaf:
		item = aNode.xField.Addr(ptr)
		integer, err := integerCellOf(item)
		if err != nil {
			return nil, err
		}
		empty, err := integer.isZero()
		if err != nil {
			return nil, err
		}
		if empty {
			return value, nil
		}
		return nil, nil
	case nodeKindArray:
		sliceLen := aNode.xSlice.Len(ptr)
		for i := 0; i < sliceLen; i++ {
			item := aNode.xSlice.ValuePointerAt(ptr, i)
			first, err := w.emptyLeaf(aNode.children, item)
			if err != nil {
				return nil, err
			}
			if first != nil {
				return first, nil
			}
		}
		return nil, nil
	}
	return item, nil
}

type integerCell struct {
	valueRef reflect.Value
	baseType reflect.Type
}

func integerCellOf(value any) (*integerCell, error) {
	actual := reflect.ValueOf(value)
	if !actual.IsValid() || actual.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("unsupported sequence field %T: expected an integer pointer", value)
	}
	baseType := actual.Type()
	for baseType.Kind() == reflect.Pointer {
		baseType = baseType.Elem()
	}
	switch baseType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &integerCell{valueRef: actual, baseType: baseType}, nil
	default:
		return nil, fmt.Errorf("unsupported sequence field %T: expected an integer pointer", value)
	}
}

func (c *integerCell) isZero() (bool, error) {
	if c == nil || !c.valueRef.IsValid() {
		return false, fmt.Errorf("sequence integer field is required")
	}
	actual := c.dereference(false)
	if !actual.IsValid() {
		return true, nil
	}
	if actual.Kind() >= reflect.Int && actual.Kind() <= reflect.Int64 {
		return actual.Int() == 0, nil
	}
	return actual.Uint() == 0, nil
}

func (c *integerCell) set(value int64) error {
	if err := c.check(value); err != nil {
		return err
	}
	if c.baseType.Kind() >= reflect.Int && c.baseType.Kind() <= reflect.Int64 {
		c.dereference(true).SetInt(value)
	} else {
		c.dereference(true).SetUint(uint64(value))
	}
	return nil
}

func (c *integerCell) integer() (int64, bool) {
	actual := c.dereference(false)
	if !actual.IsValid() {
		return 0, false
	}
	if actual.Kind() >= reflect.Int && actual.Kind() <= reflect.Int64 {
		return actual.Int(), true
	}
	value := actual.Uint()
	// Such a supplied unsigned value cannot collide with an int64 range.
	return int64(value), value <= math.MaxInt64
}

func (c *integerCell) location() uintptr {
	value := c.valueRef
	for value.Elem().Kind() == reflect.Pointer && !value.Elem().IsNil() {
		value = value.Elem()
	}
	return value.Pointer()
}

func (c *integerCell) check(value int64) error {
	if c == nil || !c.valueRef.IsValid() {
		return fmt.Errorf("sequence integer field is required")
	}
	target := reflect.New(c.baseType).Elem()
	if target.Kind() >= reflect.Int && target.Kind() <= reflect.Int64 {
		if target.OverflowInt(value) {
			return fmt.Errorf("sequence value %d overflows %v", value, c.baseType)
		}
		return nil
	}
	if value < 0 || target.OverflowUint(uint64(value)) {
		return fmt.Errorf("sequence value %d overflows %v", value, c.baseType)
	}
	return nil
}

func (c *integerCell) dereference(initialize bool) reflect.Value {
	actual := c.valueRef
	for actual.Kind() == reflect.Pointer {
		if actual.IsNil() {
			if !initialize {
				return reflect.Value{}
			}
			actual.Set(reflect.New(actual.Type().Elem()))
		}
		actual = actual.Elem()
	}
	return actual
}

func NewWalker(value interface{}, selectors []string) (*Walker, error) {
	root, err := newNode(value, selectors...)
	if err != nil {
		return nil, err
	}
	return &Walker{root: root}, nil
}
