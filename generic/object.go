package generic

import (
	"github.com/viant/toolbox"
)

//Object represents dynamic object
type Object struct {
	_proto *Proto
	_data  []interface{}
}

//Proto returns object _proto
func (o *Object) Proto() *Proto {
	return o._proto
}

//Init initialise entire object
func (o *Object) Init(values map[string]interface{}) {
	o._data = o._proto.asValues(values)
}

//AsMap return map
func (o *Object) AsMap() map[string]interface{} {
	return o._proto.asMap(o._data)
}

//SetValue sets values
func (o *Object) SetValue(name string, value interface{}) {
	field := o._proto.FieldWithValue(name, value)
	field.Set(value, &o._data)
}

//Values get value for supplied name
func (o *Object) Value(name string) interface{} {
	field := o._proto.Field(name)
	if field == nil {
		return nil
	}
	return field.Get(o._data)
}

//ValueAt get value for supplied filed Index
func (o *Object) ValueAt(index int) interface{} {
	if index >= len(o._data) {
		return nil
	}
	return Value(o._data[index])
}

//HasAt returns true if has value
func (o *Object) HasAt(index int) bool {
	if index >= len(o._data) {
		return false
	}
	return o._data[index] != nil
}

//FloatValue return float for supplied name
func (o *Object) FloatValue(name string) (*float64, error) {
	val := o.Value(name)
	if val == nil {
		return nil, nil
	}
	casted, err := toolbox.ToFloat(val)
	return &casted, err
}

//IntValue returns int value
func (o *Object) IntValue(name string) (*int, error) {
	val := o.Value(name)
	if val == nil {
		return nil, nil
	}
	casted, err := toolbox.ToInt(val)
	return &casted, err
}

//IntValue returns int value
func (o *Object) StringValue(name string) *string {
	val := o.Value(name)
	if val == nil {
		return nil
	}
	casted := toolbox.AsString(val)
	return &casted
}

//IsNil returns true if object is nil
func (u *Object) IsNil() bool {
	for i := range u._data {
		if !u._proto.fields[i].IsEmpty(u._proto, u._data[i]) {
			return false
		}
	}
	return true
}
