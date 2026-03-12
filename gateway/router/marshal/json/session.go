package json

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/francoispqt/gojay"
)

type (
	MarshallSession struct {
		Filters *Filters
		Options []interface{}
		*bytes.Buffer
		Interceptors MarshalerInterceptors
		visited      map[visitKey]int
	}

	visitKey struct {
		ptr uintptr
		typ reflect.Type
	}

	MarshalInterceptor    func() ([]byte, error)
	MarshalerInterceptors map[string]MarshalInterceptor

	UnmarshalerInterceptors map[string]UnmarshalInterceptor
	UnmarshalSession        struct {
		PathMarshaller UnmarshalerInterceptors
		Options        []interface{}
	}

	UnmarshalInterceptor func(dst interface{}, decoder *gojay.Decoder, options ...interface{}) error
)

func (s *MarshallSession) enterVisit(ptr uintptr, rType reflect.Type) error {
	if ptr == 0 {
		return nil
	}
	if s.visited == nil {
		s.visited = map[visitKey]int{}
	}
	key := visitKey{ptr: ptr, typ: rType}
	if s.visited[key] > 0 {
		return fmt.Errorf("json: unsupported value: encountered a cycle via %v", rType)
	}
	s.visited[key]++
	return nil
}

func (s *MarshallSession) leaveVisit(ptr uintptr, rType reflect.Type) {
	if ptr == 0 || s.visited == nil {
		return
	}
	key := visitKey{ptr: ptr, typ: rType}
	if s.visited[key] <= 1 {
		delete(s.visited, key)
		return
	}
	s.visited[key]--
}
