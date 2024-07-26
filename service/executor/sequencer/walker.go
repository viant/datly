package sequencer

import (
	"fmt"
	"github.com/viant/xunsafe"
	"unsafe"
)

type Walker struct {
	root *node
}

func (w *Walker) CountEmpty(value interface{}) (int, error) {
	return w.countEmpty(w.root, value)
}

// Leaf returns a leaf value
func (w *Walker) Leaf(value interface{}) (interface{}, error) {
	return w.leaf(w.root, value)
}

// Allocate allocate sequence
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
	intPtr, err := int64Ptr(item)
	if err != nil {
		return err
	}
	if *intPtr == 0 {
		*intPtr = seq.Value
		seq.Value += seq.IncrementBy
	}
	return nil
}

func (w *Walker) countEmpty(aNode *node, value interface{}) (int, error) {
	ptr := xunsafe.AsPointer(value)
	if (*unsafe.Pointer)(ptr) == nil {
		return 0, nil
	}
	var result = 0
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
	intPtr, err := int64Ptr(item)
	if err != nil {
		return 0, err
	}
	if *intPtr == 0 {
		return 1, nil
	}
	return 0, nil
}

func (w *Walker) leaf(aNode *node, value interface{}) (interface{}, error) {
	ptr := xunsafe.AsPointer(value)
	var item interface{}
	switch aNode.kind {
	case nodeKindObject:
		item = aNode.xField.Interface(ptr)
		return w.leaf(aNode.children, item)
	case nodeKindLeaf:
		return value, nil
	case nodeKindArray:
		sliceLen := aNode.xSlice.Len(ptr)
		for i := 0; i < sliceLen; {
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

func int64Ptr(value interface{}) (*int64, error) {
	switch actual := value.(type) {
	case *int, *uint, uint64:
		ptr := xunsafe.AsPointer(value)
		return (*int64)(ptr), nil
	case **int64:
		if *actual == nil {
			i := int64(0)
			*actual = &i
		}
		return *actual, nil
	case **int, **uint, **uint64:
		ptr := (**int64)(xunsafe.AsPointer(value))
		if *ptr == nil {
			i := int64(0)
			*ptr = &i
		}
		return *ptr, nil
	case *int64:
		return actual, nil
	default:
		return nil, fmt.Errorf("unspported %T, expected *int", actual)
	}
}

func NewWalker(value interface{}, selectors []string) (*Walker, error) {
	root, err := newNode(value, selectors...)
	if err != nil {
		return nil, err
	}
	return &Walker{root: root}, nil
}
