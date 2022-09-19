package csv

import (
	"fmt"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
	"unsafe"
)

type Session struct {
	buffer     []string
	objects    []*Object
	pathIndex  map[string]int
	parentNode *Object
	dest       interface{}
	destPtr    unsafe.Pointer
}

func (s *Session) init(fields []*Field, refs map[string][]string, accessors map[string]*xunsafe.Field) error {
	s.destPtr = xunsafe.AsPointer(s.dest)
	s.buffer = make([]string, len(fields))
	for i, field := range fields {
		object := s.getOrCreatePathIndex(field, refs, accessors)
		object.AddHolder(field, &s.buffer[i])
	}

	parentNode, ok := s.buildParentNode()
	if !ok {
		return fmt.Errorf("none of the parent fields were specified")
	}

	anObject, ok := parentNode.(*Object)
	if !ok {
		return fmt.Errorf("unexpected node type, wanted %T got %T", anObject, parentNode)
	}

	s.parentNode = anObject
	return nil
}

func (s *Session) getOrCreatePathIndex(field *Field, refs map[string][]string, accessors map[string]*xunsafe.Field) *Object {
	index, ok := s.pathIndex[field.path]
	if ok {
		return s.objects[index]
	}

	s.pathIndex[field.path] = len(s.objects)
	dest, appender := s.destWithAppender(field)
	var slice *xunsafe.Slice
	xField := accessors[field.path]
	if xField != nil && xField.Type.Kind() == reflect.Slice {
		slice = xunsafe.NewSlice(xField.Type)
	}

	parentID := field.path
	if parentPathIndex := strings.LastIndexByte(parentID, '.'); parentPathIndex != -1 {
		parentID = field.path[:parentPathIndex]
	} else {
		parentID = ""
	}

	object := &Object{
		objType:  field.parentType,
		path:     field.path,
		parentID: asInterface(field.path, parentID),
		dest:     dest,
		appender: appender,
		index: &Index{
			positionInSlice: map[interface{}]int{},
			data:            nil,
			objectIndex:     ObjectIndex{},
			appenders:       map[interface{}]*xunsafe.Appender{},
		},
		xField: xField,
		xSlice: slice,
	}

	s.objects = append(s.objects, object)
	return object
}

func asInterface(fieldPath string, parentID string) interface{} {
	if fieldPath == "" {
		return nil
	}

	return parentID
}

func (s *Session) addRecord(record []string) error {
	copy(s.buffer, record)
	return s.parentNode.Build()
}

func (s *Session) buildParentNode() (Node, bool) {
	nodes := make([]Node, 0, len(s.objects))
	for i, _ := range s.objects {
		nodes = append(nodes, s.objects[i])
	}

	parents := BuildTree(nodes)
	for _, parent := range parents {
		if parent.ID() == "" {
			return parent, true
		}
	}

	return nil, false
}

func (s *Session) destWithAppender(field *Field) (interface{}, *xunsafe.Appender) {
	if field.path == "" {
		dest := s.dest
		slice := xunsafe.NewSlice(reflect.TypeOf(dest).Elem())
		return dest, slice.Appender(xunsafe.AsPointer(dest))
	}

	parentType := field.parentType
	if parentType.Kind() != reflect.Slice {
		parentType = reflect.SliceOf(parentType)
	}

	dest := reflect.New(field.parentType).Interface()
	return dest, xunsafe.NewSlice(parentType).Appender(xunsafe.AsPointer(dest))
}
