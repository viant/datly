package generic

import (
	"github.com/viant/toolbox"
	"reflect"
	"time"
)

const (
	FieldTypeInt    = "int"
	FieldTypeFloat  = "float"
	FieldTypeBool   = "bool"
	FieldTypeString = "string"
	FieldTypeTime   = "time"
	FieldTypeBytes  = "bytes"
	FieldTypeArray  = "array"
	FieldTypeObject = "object"
)

//NilValue is used to discriminate between unset fileds, and set filed with nil value (for patch operation)
var NilValue = make([]*interface{}, 1)[0]

//Field represents dynamic filed
type Field struct {
	Name          string
	Index         int
	OmitEmpty     *bool
	DateFormat    string
	DataLayout    string
	DataType      string
	InputName     string
	ComponentType string
	provider      *Provider
	outputName    string
	hidden        bool
}

func (f *Field) IsEmpty(proto *Proto, value interface{}) bool {
	if value == nil || value == NilValue {
		return true
	}
	if !f.ShallOmitEmpty(proto) {
		return false
	}
	if nillable, ok := value.(Nilable); ok {
		return nillable.IsNil()
	}
	switch f.DataType {
	case FieldTypeBool:
		if !toolbox.AsBoolean(value) {
			return true
		}
	case FieldTypeInt:
		if toolbox.AsInt(value) == 0 {
			return true
		}
	case FieldTypeFloat:
		if toolbox.AsFloat(value) == 0 {
			return true
		}
	case FieldTypeString:
		if toolbox.AsString(value) == "" {
			return true
		}
	}
	if toolbox.IsSlice(value) {
		return reflect.ValueOf(value).Len() == 0
	}
	return false
}

func (f *Field) ShallOmitEmpty(proto *Proto) bool {
	if f.OmitEmpty == nil {
		return proto.OmitEmpty
	}
	return *f.OmitEmpty
}

func (f *Field) TimeLayout(proto *Proto) string {
	if f.DataLayout == "" {
		return proto.timeLayout
	}
	return f.DataLayout
}

func (f *Field) InitType(value interface{}) {
	if value == nil {
		return
	}
	switch val := value.(type) {
	case *Object:
		f.DataType = FieldTypeObject
		if val == nil {
			f.provider = NewProvider()
			return
		}
		f.provider = &Provider{Proto: val.Proto()}
		return
	case *Array:
		f.DataType = FieldTypeArray
		if val == nil {
			f.provider = NewProvider()
			return
		}
		f.provider = &Provider{Proto: val.Proto()}
		return
	case *Map:
		f.DataType = FieldTypeArray
		if val == nil {
			f.provider = NewProvider()
			return
		}
		f.provider = &Provider{Proto: val.Proto()}
		return
	case *Multimap:
		f.DataType = FieldTypeArray
		if val == nil {
			f.provider = NewProvider()
			return
		}
		f.provider = &Provider{Proto: val.Proto()}
		return
	case time.Time, *time.Time, **time.Time, string, []byte:
		f.DataType = getBaseType(value)
		return

	default:
		f.DataType = getBaseType(value)
	}

	if toolbox.IsMap(value) || toolbox.IsStruct(value) {
		f.provider = NewProvider()
		f.DataType = FieldTypeObject
		return
	}
	if toolbox.IsSlice(value) {
		f.provider = NewProvider()
		f.DataType = FieldTypeArray
		componentType := toolbox.DiscoverComponentType(value)
		componentValue := reflect.New(componentType).Interface()
		f.ComponentType = getBaseType(componentValue)
		return
	}

}

func getBaseType(value interface{}) string {
	switch val := value.(type) {
	case float32, float64, *float32, *float64:
		return FieldTypeFloat
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, *int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64:
		return FieldTypeInt
	case time.Time, *time.Time:
		return FieldTypeTime
	case bool, *bool:
		return FieldTypeBool
	case []byte:
		if _, err := toolbox.ToFloat(val); err == nil {
			return FieldTypeFloat
		}
	}
	return FieldTypeString
}

//Set sets a field value
func (f *Field) Set(value interface{}, result *[]interface{}) {
	if value != nil {
		if toolbox.IsSlice(value) {
			slice := toolbox.AsSlice(value)
			if len(slice) > 0 && toolbox.IsMap(slice[0]) {
				value = f.provider.NewArray(slice...)
			}

		} else if toolbox.IsMap(value) {
			object := f.provider.NewObject()
			object.Init(toolbox.AsMap(value))
		}
	}
	if value == nil {
		value = NilValue
	}
	values := *result
	values = reallocateIfNeeded(f.Index+1, values)
	values[f.Index] = value
	*result = values
}

func (f *Field) OutputName() string {
	if f.outputName == "" {
		return f.Name
	}
	return f.outputName
}

//Get returns field value
func (f *Field) Get(values []interface{}) interface{} {
	if f.Index < len(values) {
		return Value(values[f.Index])
	}
	return nil
}
