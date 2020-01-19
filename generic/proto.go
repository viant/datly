package generic

import (
	"github.com/viant/toolbox"
	"reflect"
	"sync"
)

//Proto represents generic type prototype
type Proto struct {
	lock       *sync.RWMutex
	fieldNames map[string]*Field
	fields     []*Field
}

//Size returns proto size
func (s *Proto) Size() int {
	s.lock.RLock()
	result := len(s.fieldNames)
	s.lock.RUnlock()
	return result
}

func (s *Proto) asValues(values map[string]interface{}) []interface{} {
	var result = make([]interface{}, len(values))
	if len(values) == 0 {
		return result
	}
	for k, v := range values {
		field := s.getField(k, v)
		field.Set(v, &result)
	}
	return result
}

func (s *Proto) asMap(values []interface{}) map[string]interface{} {
	var result = make(map[string]interface{})
	for _, field := range s.fields {
		var value interface{}
		if field.index < len(values) {
			value = values[field.index]
		}
		result[field.Name] = value
	}
	return result
}

func reallocateIfNeeded(size int, data []interface{}) []interface{} {
	if size >= len(data) {
		for i := len(data); i < size; i++ {
			data = append(data, nil)
		}
	}
	return data
}

//Fields returns fields list
func (s *Proto) Fields() []*Field {
	return s.fields
}

//Field returns field for specified name
func (s *Proto) Field(name string) *Field {
	s.lock.RLock()
	field := s.fieldNames[name]
	s.lock.RUnlock()
	return field
}

//getField returns existing filed , or create a new field
func (s *Proto) getField(fieldName string, value interface{}) *Field {
	s.lock.RLock()
	field, ok := s.fieldNames[fieldName]
	s.lock.RUnlock()
	if ok {
		return field
	}
	if value != nil && toolbox.IsMap(value) && toolbox.IsSlice(value) {
		field.provider = NewProvider()
	}
	field = &Field{Name: fieldName, index: len(s.fieldNames), Type: reflect.TypeOf(value)}
	s.lock.Lock()
	defer s.lock.Unlock()
	s.fieldNames[fieldName] = field
	s.fields = append(s.fields, field)
	return field
}

//newProto create a data type prototype
func newProto() *Proto {
	return &Proto{
		lock:       &sync.RWMutex{},
		fieldNames: make(map[string]*Field),
		fields:     make([]*Field, 0),
	}
}
