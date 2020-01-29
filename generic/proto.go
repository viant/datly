package generic

import (
	"github.com/pkg/errors"
	"github.com/viant/toolbox"
	"reflect"
	"sync"
)

//Proto represents generic type prototype
type Proto struct {
	lock             *sync.RWMutex
	fieldNames       map[string]*Field
	fields           []*Field
	caseFormat       int
	outputCaseFormat int
	inputCaseFormat  int
}

//OutputCaseFormat set output case format
func (s *Proto) OutputCaseFormat(source, output string) error {
	var ok bool
	s.caseFormat, ok = CaseFormat[source]
	if !ok {
		return errors.Errorf("invalid case format: %v", source)
	}
	s.outputCaseFormat, ok = CaseFormat[output]
	if !ok {
		return errors.Errorf("invalid output case format: %v", output)
	}
	for i, field := range s.fields {
		s.fields[i].outputName = toolbox.ToCaseFormat(field.Name, s.caseFormat, s.outputCaseFormat)
	}
	return nil
}

//InputCaseFormat set output case format
func (s *Proto) InputCaseFormat(source, input string) error {
	var ok bool
	s.caseFormat, ok = CaseFormat[source]
	if !ok {
		return errors.Errorf("invalid case format: %v", source)
	}
	s.inputCaseFormat, ok = CaseFormat[input]
	if !ok {
		return errors.Errorf("invalid input case format: %v", input)
	}
	return nil
}

//Hide set hidden flag for the field
func (s *Proto) Hide(name string) {
	field := s.Field(name)
	if field == nil {
		return
	}
	field.hidden = true
}

//Show remove hidden flag for supplied field
func (s *Proto) Show(name string) {
	field := s.Field(name)
	if field == nil {
		return
	}
	field.hidden = false
}

//Size returns _proto size
func (s *Proto) Size() int {
	s.lock.RLock()
	result := len(s.fields)
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
		if field.hidden {
			continue
		}
		var value interface{}
		if field.Index < len(values) {
			value = values[field.Index]
		}
		value = Value(value)
		fieldName := field.Name
		if field.outputName != "" {
			fieldName = field.outputName
		}
		result[fieldName] = value
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

	inputFieldName := ""
	if s.inputCaseFormat != s.caseFormat {
		inputFieldName = fieldName
		fieldName = toolbox.ToCaseFormat(fieldName, s.inputCaseFormat, s.caseFormat)
	}

	field = &Field{Name: fieldName, Index: len(s.fieldNames), Type: reflect.TypeOf(value)}
	if s.caseFormat != s.outputCaseFormat {
		field.outputName = toolbox.ToCaseFormat(field.Name, s.caseFormat, s.outputCaseFormat)
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	s.fieldNames[fieldName] = field
	if inputFieldName != "" {
		s.fieldNames[inputFieldName] = field
	}
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
