package sequencer

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
)

type nodeKind int

const (
	nodeKindUnknown = nodeKind(0)
	nodeKindLeaf    = nodeKind(1)
	nodeKindObject  = nodeKind(2)
	nodeKindArray   = nodeKind(3)
)

type node struct {
	kind      nodeKind
	ownerType reflect.Type
	xField    *xunsafe.Field
	xSlice    *xunsafe.Slice
	children  *node
}

func newNode(value interface{}, selectors ...string) (*node, error) {
	aNode := &node{}
	var err error
	aNode.ownerType = reflect.TypeOf(value)
	rawType := aNode.ownerType
	if aNode.ownerType.Kind() == reflect.Ptr {
		rawType = aNode.ownerType.Elem()
	}
	switch len(selectors) {
	case 0:
		return nil, fmt.Errorf("selector was empty")
	case 1:
		aNode.kind = nodeKindLeaf
	}

	switch rawType.Kind() {
	case reflect.Slice:
		aNode.kind = nodeKindArray
		aNode.xSlice = xunsafe.NewSlice(rawType)
		itemValue := reflect.New(rawType.Elem()).Elem().Interface()
		aNode.children, err = newNode(itemValue, selectors...)

		return aNode, err
	case reflect.Struct:
		if aNode.xField = xunsafe.FieldByName(rawType, selectors[0]); aNode.xField == nil {
			return nil, fmt.Errorf("failed to lookup field: %v on %v", selectors[0], rawType.Name())
		}
		if aNode.kind == nodeKindLeaf {
			return aNode, nil
		}
		aNode.kind = nodeKindObject
		itemValue := reflect.New(aNode.xField.Type).Elem().Interface()
		if aNode.children, err = newNode(itemValue, selectors[1:]...); err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("!unsupported type:%T", value)
	}
	return aNode, nil
}
