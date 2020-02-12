package generic

import (
	"github.com/pkg/errors"
	"github.com/viant/toolbox"
	"sync"
	"time"
)

var defaultEmptyValues = map[interface{}]bool{
	"":  true,
	nil: true,
}

//Proto represents generic type prototype
type Proto struct {
	lock             *sync.RWMutex
	fieldNames       map[string]*Field
	fields           []*Field
	nilTypes         []int
	OmitEmpty        bool
	emptyValues      map[interface{}]bool
	timeLayout       string
	caseFormat       int
	outputCaseFormat int
	inputCaseFormat  int
}

//SetOmitEmpty sets omit empty flag
func (s *Proto) SetOmitEmpty(omitEmpty bool) {
	s.OmitEmpty = true
	if omitEmpty {
		if len(s.emptyValues) == 0 {
			s.emptyValues = defaultEmptyValues
		}
	}
}

//SetEmptyValues sets empty values, use only if empty values are non in default map: nil, empty string
func (s *Proto) SetEmptyValues(values ...interface{}) {
	s.emptyValues = make(map[interface{}]bool)
	for i := range values {
		s.emptyValues[values[i]] = true
	}
}

//OutputCaseFormat set output case format
func (p *Proto) OutputCaseFormat(source, output string) error {
	var ok bool
	p.caseFormat, ok = CaseFormat[source]
	if !ok {
		return errors.Errorf("invalid case format: %v", source)
	}
	p.outputCaseFormat, ok = CaseFormat[output]
	if !ok {
		return errors.Errorf("invalid output case format: %v", output)
	}
	for i, field := range p.fields {
		p.fields[i].outputName = toolbox.ToCaseFormat(field.Name, p.caseFormat, p.outputCaseFormat)
	}
	return nil
}

//InputCaseFormat set output case format
func (p *Proto) InputCaseFormat(source, input string) error {
	var ok bool
	p.caseFormat, ok = CaseFormat[source]
	if !ok {
		return errors.Errorf("invalid case format: %v", source)
	}
	p.inputCaseFormat, ok = CaseFormat[input]
	if !ok {
		return errors.Errorf("invalid input case format: %v", input)
	}
	return nil
}

//Hide set hidden flag for the field
func (p *Proto) Hide(name string) {
	field := p.Field(name)
	if field == nil {
		return
	}
	field.hidden = true
}

//Show remove hidden flag for supplied field
func (p *Proto) Show(name string) {
	field := p.Field(name)
	if field == nil {
		return
	}
	field.hidden = false
}

//Size returns _proto size
func (p *Proto) Size() int {
	p.lock.RLock()
	result := len(p.fields)
	p.lock.RUnlock()
	return result
}

func (p *Proto) asValues(values map[string]interface{}) []interface{} {
	var result = make([]interface{}, len(values))
	if len(values) == 0 {
		return result
	}
	for k := range values {
		field := p.FieldWithValue(k, values[k])
		field.Set(values[k], &result)
	}
	return result
}

func (p *Proto) asMap(values []interface{}) map[string]interface{} {
	var result = make(map[string]interface{})
	for _, field := range p.fields {
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
func (p *Proto) Fields() []*Field {
	return p.fields
}

//Field returns field for specified name
func (p *Proto) Field(name string) *Field {
	p.lock.RLock()
	field := p.fieldNames[name]
	p.lock.RUnlock()
	return field
}

//Object creates an object
func (p *Proto) Object(values []interface{}) (*Object, error) {
	if len(p.fields) < len(values) {
		return nil, errors.Errorf("invalid value count: %v, field count: %v", len(values), len(p.fields))
	}

	object := &Object{_proto: p, _data: values}
	if len(p.nilTypes) > 0 {
		for _, index := range p.nilTypes {
			if values[index] != nil {
				p.fields[index].InitType(values[index])
			}
		}
		p.updateNilTypes()
	}
	return object, nil
}

//FieldWithValue returns existing filed , or create a new field
func (p *Proto) FieldWithValue(fieldName string, value interface{}) *Field {
	p.lock.RLock()
	field, ok := p.fieldNames[fieldName]
	p.lock.RUnlock()
	if ok {
		return field
	}

	inputFieldName := ""
	if p.inputCaseFormat != p.caseFormat {
		inputFieldName = fieldName
		fieldName = toolbox.ToCaseFormat(fieldName, p.inputCaseFormat, p.caseFormat)
	}
	field = &Field{Name: fieldName, Index: len(p.fieldNames), inputName: inputFieldName}
	field.InitType(value)
	if p.caseFormat != p.outputCaseFormat {
		field.outputName = toolbox.ToCaseFormat(field.Name, p.caseFormat, p.outputCaseFormat)
	}
	if value == nil {
		p.addNilType(field.Index)
	}

	return p.AddField(field)
}

func (p *Proto) AddField(field *Field) *Field {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.fieldNames[field.Name] = field
	if field.inputName != "" {
		p.fieldNames[field.inputName] = field
	}
	if field.outputName != "" {
		p.fieldNames[field.outputName] = field
	}
	p.fields = append(p.fields, field)
	return field
}

func (p *Proto) updateNilTypes() {
	p.nilTypes = make([]int, 0)
	for i := range p.fields {
		if p.fields[i].DataType == "" {
			p.nilTypes = append(p.nilTypes, p.fields[i].Index)
		}
	}
}

func (p *Proto) addNilType(index int) {
	if len(p.nilTypes) == 0 {
		p.nilTypes = make([]int, 0)
	}
	p.nilTypes = append(p.nilTypes, index)
}

//newProto create a data type prototype
func newProto(fields ...*Field) *Proto {
	result := &Proto{
		lock:       &sync.RWMutex{},
		fieldNames: make(map[string]*Field),
		fields:     make([]*Field, 0),
	}
	result.timeLayout = time.RFC3339
	for i := range fields {
		result.AddField(fields[i])
	}
	return result
}
